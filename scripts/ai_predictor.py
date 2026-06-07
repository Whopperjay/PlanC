#!/usr/bin/env python3
"""
F1 AI Predictor ("Pitwall") using Ollama.

Generates top-3 predictions for each upcoming GP session (qualifying, sprint
qualifying, sprint, grand prix) based on current standings, circuit info and the
latest news, then writes data/ai_predictions.json.

Model is configurable via OLLAMA_MODEL (default qwen2.5:7b — strong FR/EN).
"""

import json
import os
import re
import requests
import time
from datetime import datetime, timezone, timedelta

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:7b")

# LLM provider: "groq" -> Groq OpenAI-compatible cloud API (no local RAM);
# anything else -> local Ollama /api/generate (fallback, instant rollback).
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "ollama").lower()
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_PRED_MODEL = os.environ.get("GROQ_PRED_MODEL", "llama-3.3-70b-versatile")  # predictions: strong model, low volume
GROQ_URL = os.environ.get("GROQ_URL", "https://api.groq.com/openai/v1/chat/completions")
GROQ_MIN_INTERVAL = float(os.environ.get("GROQ_MIN_INTERVAL", "6.0"))  # sec between calls (free-tier RPM)
GROQ_MAX_RETRIES = int(os.environ.get("GROQ_MAX_RETRIES", "5"))
ACTIVE_MODEL = GROQ_PRED_MODEL if LLM_PROVIDER == "groq" else OLLAMA_MODEL

_GROQ_LAST_CALL = [0.0]  # module-level throttle state

# session types we support
SESSION_TYPES = ["qualifying", "grand_prix", "sprint", "sprint_qualifying"]


def _load(path: str):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _standings_text(standings_data: dict) -> str:
    try:
        sl = (standings_data.get("MRData", {})
              .get("StandingsTable", {})
              .get("StandingsLists", [{}])[0]
              .get("DriverStandings", []))
        lines = []
        for s in sl[:10]:
            d = s.get("Driver", {})
            name = f"{d.get('givenName','')} {d.get('familyName','')}".strip()
            did = d.get("driverId", "")
            pts = s.get("points", "0")
            pos = s.get("position", "?")
            team = s.get("Constructors", [{}])[0].get("name", "")
            lines.append(f"P{pos}: {name} (id={did}, {team}) — {pts} pts")
        return "\n".join(lines)
    except Exception:
        return "Standings indisponibles"


def _news_context(news_data: dict, n: int = 8) -> str:
    articles = (news_data or {}).get("articles", [])[:n]
    lines = []
    for a in articles:
        title = a.get("title_fr") or a.get("title", "")
        summary = (a.get("summary_fr") or a.get("summary_en") or "")[:150]
        src = a.get("source", "")
        lines.append(f"• [{src}] {title}\n  {summary}")
    return "\n\n".join(lines) if lines else "Aucune actualité disponible"


def _session_description(stype: str) -> str:
    return {
        "qualifying": "Qualifications (tour rapide, 1 tour lancé, pas de pitstop, track evolution importante)",
        "grand_prix": "Course Grand Prix (stratégie pneus, pitstops, safety car, dégradation, 50+ tours)",
        "sprint": "Course Sprint (100km, pas de pitstop obligatoire, départ grille sprint, intense)",
        "sprint_qualifying": "Qualifications Sprint (session courte pour grille sprint, très haute intensité)",
    }.get(stype, stype)


def _get_upcoming_sessions(schedule: dict, data_dir: str) -> list:
    """
    Parse current_schedule.json to find sessions in the next 72h.
    Returns list of dicts with session_type, session_date, race_name, circuit, race_id.
    """
    now = datetime.now(timezone.utc)
    window_hours = int(os.environ.get("PRED_WINDOW_HOURS", "72"))
    cutoff = now + timedelta(hours=window_hours)
    results = []

    try:
        races = (schedule.get("MRData", {})
                 .get("RaceTable", {})
                 .get("Races", []))
    except Exception:
        return results

    for race in races:
        rname = race.get("raceName", "")
        circuit = race.get("Circuit", {}).get("circuitName", "")
        race_id = (rname.lower()
                   .replace(" grand prix", "").replace(" ", "_")
                   .replace("'", "").replace("-", "_"))

        sessions_to_check = [
            ("date", "time", "grand_prix"),
            ("Qualifying", None, "qualifying"),
            ("Sprint", None, "sprint"),
            ("SprintQualifying", None, "sprint_qualifying"),
        ]

        for date_key, time_key, stype in sessions_to_check:
            if date_key == "date":
                s_date = race.get("date", "")
                s_time = race.get("time", "15:00:00Z")
                raw = f"{s_date}T{s_time}"
            else:
                session = race.get(date_key)
                if not session:
                    continue
                s_date = session.get("date", "")
                s_time = session.get("time", "15:00:00Z")
                raw = f"{s_date}T{s_time}"

            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except Exception:
                continue

            if now < dt <= cutoff:
                event_id = f"{race_id}_{stype}"
                results.append({
                    "event_id": event_id,
                    "session_type": stype,
                    "session_date": dt.isoformat(),
                    "race_name": rname,
                    "circuit": circuit,
                    "race_id": race_id,
                })

    # Restrict to the SINGLE next race weekend (never predict the following GP).
    gp = [r for r in results if r["session_type"] == "grand_prix"]
    anchor = gp if gp else results
    if anchor:
        next_race_id = min(anchor, key=lambda r: r["session_date"])["race_id"]
        results = [r for r in results if r["race_id"] == next_race_id]
    return results


def _call_ollama(prompt: str, timeout: int = 120) -> dict:
    try:
        if LLM_PROVIDER == "groq":
            raw = "{}"
            for attempt in range(GROQ_MAX_RETRIES):
                wait = GROQ_MIN_INTERVAL - (time.time() - _GROQ_LAST_CALL[0])
                if wait > 0:
                    time.sleep(wait)
                resp = requests.post(
                    GROQ_URL,
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                    json={
                        "model": GROQ_PRED_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "response_format": {"type": "json_object"},
                        "temperature": 0.15,
                    },
                    timeout=timeout,
                )
                _GROQ_LAST_CALL[0] = time.time()
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("retry-after", 2 ** attempt))
                    print(f"    Groq 429 rate-limited, retry in {retry_after:.1f}s ({attempt + 1}/{GROQ_MAX_RETRIES})")
                    time.sleep(min(retry_after, 30))
                    continue
                resp.raise_for_status()
                raw = resp.json()["choices"][0]["message"]["content"]
                break
        else:
            resp = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
                timeout=timeout,
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "{}")
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            raw = m.group(0)
        return json.loads(raw)
    except Exception as e:
        print(f"    Ollama error: {e}")
        return {}



LOW_OVERTAKING = ("monaco", "singapore", "hungar", "zandvoort", "imola", "marina bay", "monte")


def _valid_ids(standings_data: dict) -> list:
    try:
        sl = standings_data["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
        return [s["Driver"]["driverId"] for s in sl]
    except Exception:
        return []


def _recent_form(data_dir: str) -> str:
    out = []
    lr = _load(os.path.join(data_dir, "last_results.json"))
    try:
        r = lr["MRData"]["RaceTable"]["Races"][-1]
        res = r.get("Results") or []
        if res:
            out.append("Derniere course terminee (" + r.get("raceName", "") + ") :")
            for x in res[:10]:
                out.append("  P%s: %s (%s)" % (x.get("position"), x["Driver"]["driverId"], x.get("status", "")))
    except Exception:
        pass
    q = _load(os.path.join(data_dir, "qualifying.json"))
    try:
        r = q["MRData"]["RaceTable"]["Races"][-1]
        res = r.get("QualifyingResults") or []
        if res:
            out.append("Derniere qualif (" + r.get("raceName", "") + ") :")
            for x in res[:10]:
                out.append("  P%s: %s" % (x.get("position"), x["Driver"]["driverId"]))
    except Exception:
        pass
    return "\n".join(out) if out else "Pas de resultats recents disponibles"


def _quali_grid_live(race_name: str) -> str:
    """Starting grid = this weekend's qualifying result, pulled from the live feed
    (the results pipeline lags by ~1 race)."""
    try:
        import live_timing as lt
        yr = datetime.now(timezone.utc).year
        idx = json.loads(lt._get(f"{lt.BASE}/{yr}/Index.json").text.lstrip(lt.BOM))
        key = race_name.lower().replace(" grand prix", "").strip()
        for m in idx.get("Meetings", []):
            if key and key in m.get("Name", "").lower():
                for sess in m.get("Sessions", []):
                    if sess.get("Name") == "Qualifying" and sess.get("Path"):
                        lb = lt.build_once(sess["Path"], m.get("Name", ""))
                        rows = lb.get("drivers") or []
                        if rows:
                            lines = []
                            for d in rows[:12]:
                                tag = " (pole)" if d["pos"] == 1 else ""
                                lines.append("  P%s%s: %s [%s]" % (d["pos"], tag, d.get("name", ""), d.get("best", "")))
                            return "\n".join(lines)
    except Exception as e:
        print("    quali grid (live) unavailable:", e)
    return ""


def _generate_one(session: dict, standings_txt: str, news_txt: str,
                  recent_form_txt: str = "", valid_ids: list = None, grid_txt: str = ""):
    is_race = session["session_type"] in ("grand_prix", "sprint")
    circ = session["circuit"].lower()
    track_note = ""
    if any(k in circ for k in LOW_OVERTAKING) or any(k in session["race_name"].lower() for k in LOW_OVERTAKING):
        track_note = "ATTENTION: circuit ou depasser est tres difficile -> la position sur la grille est decisive pour la course."
    grid_block = ""
    if is_race and grid_txt:
        grid_block = "\n=== GRILLE DE DEPART (qualif de CE week-end) ===\n" + grid_txt + "\n"
    ids_line = ", ".join(valid_ids or [])

    prompt = f"""Tu es le "Pitwall", analyste F1 de pointe. Pronostique le TOP 3 de la session avec rigueur, en t'appuyant STRICTEMENT sur les donnees.

=== SESSION ===
Type : {_session_description(session['session_type'])}
Course : {session['race_name']}  |  Circuit : {session['circuit']}
Date UTC : {session['session_date']}
{track_note}
{grid_block}
=== CLASSEMENT CHAMPIONNAT (top 10) ===
{standings_txt}

=== FORME RECENTE ===
{recent_form_txt}

=== ACTUALITES (48h) ===
{news_txt}

=== METHODE ===
- COURSE : la grille de depart est le facteur n.1 (surtout sur circuit difficile a depasser). Ajuste avec rythme de course, degradation pneus, strategie, fiabilite, meteo.
- QUALIF : privilegie le rythme sur un tour, la forme en qualif recente et l'evolution de piste.
- Reste coherent avec les donnees: ne place pas en tete un pilote sans rythme recent.
- N'invente JAMAIS de pilote. Utilise UNIQUEMENT ces driver_id valides : {ids_line}

Reponds UNIQUEMENT avec ce JSON (rien d'autre) :
{{"p1":"driver_id","p2":"driver_id","p3":"driver_id","reasoning_fr":"Explication ~80 mots en francais","reasoning_en":"Explanation ~80 words in English","confidence":"high|medium|low","key_factor_fr":"Facteur cle ~10 mots"}}"""

    parsed = _call_ollama(prompt)
    if not parsed or not parsed.get("p1"):
        return None

    return {
        "event_id": session["event_id"],
        "race_id": session["race_id"],
        "session_type": session["session_type"],
        "session_date": session["session_date"],
        "p1": str(parsed.get("p1", "")).strip(),
        "p2": str(parsed.get("p2", "")).strip(),
        "p3": str(parsed.get("p3", "")).strip(),
        "reasoning_fr": str(parsed.get("reasoning_fr", ""))[:400],
        "reasoning_en": str(parsed.get("reasoning_en", ""))[:400],
        "confidence": str(parsed.get("confidence", "medium")),
        "key_factor_fr": str(parsed.get("key_factor_fr", ""))[:80],
        "news_used_ids": [],
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def generate_ai_predictions(data_dir: str) -> bool:
    print(f"\n=== PITWALL PREDICTOR starting at {datetime.now()} (provider={LLM_PROVIDER}, model={ACTIVE_MODEL}) ===")

    pred_file = os.path.join(data_dir, "ai_predictions.json")
    schedule = _load(os.path.join(data_dir, "current_schedule.json"))
    standings = _load(os.path.join(data_dir, "driver_standings.json"))
    news = _load(os.path.join(data_dir, "news.json")) or {"articles": []}

    if not schedule or not standings:
        print("  Missing schedule or standings — skipping")
        return False

    existing = {}
    if os.path.exists(pred_file):
        try:
            old = _load(pred_file) or {}
            for p in old.get("predictions", []):
                existing[p["event_id"]] = p
        except Exception:
            pass

    upcoming = _get_upcoming_sessions(schedule, data_dir)
    if not upcoming:
        print("  No sessions in next 72h")
        return False

    standings_txt = _standings_text(standings)
    news_txt = _news_context(news)
    recent_form_txt = _recent_form(data_dir)
    valid_ids = _valid_ids(standings)
    _grid_cache = {}

    new_preds = dict(existing)
    generated = 0

    for session in upcoming:
        eid = session["event_id"]
        if eid in existing:
            try:
                gen_at = datetime.fromisoformat(
                    existing[eid].get("generated_at", "2000-01-01T00:00:00Z")
                    .replace("Z", "+00:00")
                )
                age_h = (datetime.now(timezone.utc) - gen_at).total_seconds() / 3600
                if age_h < 12:
                    print(f"  Skip {eid} (prediction {age_h:.0f}h old)")
                    continue
            except Exception:
                pass

        print(f"  Generating → {eid}")
        grid_txt = ""
        if session["session_type"] in ("grand_prix", "sprint"):
            if session["race_id"] not in _grid_cache:
                _grid_cache[session["race_id"]] = _quali_grid_live(session["race_name"])
            grid_txt = _grid_cache[session["race_id"]]
        pred = _generate_one(session, standings_txt, news_txt, recent_form_txt, valid_ids, grid_txt)
        if pred:
            new_preds[eid] = pred
            generated += 1
            print(f"    P1={pred['p1']} P2={pred['p2']} P3={pred['p3']} [{pred['confidence']}]")
            time.sleep(1)

    if generated == 0 and os.path.exists(pred_file):
        print("  No new predictions")
        return False

    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "model": ACTIVE_MODEL,
        "predictions": list(new_preds.values()),
    }
    with open(pred_file, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"=== PITWALL PREDICTOR done: {generated} predictions ===\n")
    return generated > 0


if __name__ == "__main__":
    _data = os.environ.get("DATA_DIR", "data")
    if not os.path.exists(_data):
        os.makedirs(_data)
    generate_ai_predictions(_data)
