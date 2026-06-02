#!/usr/bin/env python3
"""
F1 AI Predictor using Ollama llama3.
Generates top-3 predictions for each upcoming GP session
based on current standings, circuit info, and latest news.
"""

import json
import os
import re
import requests
import time
from datetime import datetime, timezone, timedelta

# LLM provider: "groq" -> Groq OpenAI-compatible cloud (no local RAM);
# anything else -> local Ollama /api/generate (fallback, instant rollback).
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "ollama").lower()
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_URL = os.environ.get("GROQ_URL", "https://api.groq.com/openai/v1/chat/completions")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3:latest")  # fallback only
GROQ_MIN_INTERVAL = float(os.environ.get("GROQ_MIN_INTERVAL", "2.0"))
GROQ_MAX_RETRIES = int(os.environ.get("GROQ_MAX_RETRIES", "5"))
ACTIVE_MODEL = GROQ_MODEL if LLM_PROVIDER == "groq" else OLLAMA_MODEL
_GROQ_LAST_CALL = [0.0]


def _groq_chat(prompt, temperature=0.15, timeout=120):
    """Call Groq with throttling + 429 backoff (respects Retry-After)."""
    for attempt in range(GROQ_MAX_RETRIES):
        wait = GROQ_MIN_INTERVAL - (time.time() - _GROQ_LAST_CALL[0])
        if wait > 0:
            time.sleep(wait)
        resp = requests.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
                "temperature": temperature,
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
        return resp.json()["choices"][0]["message"]["content"]
    resp.raise_for_status()
    return "{}"


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
        title = a.get("title", "")
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
    cutoff = now + timedelta(hours=72)
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
            if date_key in ("date", "date"):
                # Main race
                if date_key == "date":
                    s_date = race.get("date", "")
                    s_time = race.get("time", "15:00:00Z") if time_key is None else race.get("time", "15:00:00Z")
                    raw = f"{s_date}T{s_time}"
                else:
                    continue
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

    return results


def _call_ollama(prompt: str, ollama_url: str, timeout: int = 120) -> dict:
    try:
        if LLM_PROVIDER == "groq":
            raw = _groq_chat(prompt, timeout=timeout)
        else:
            resp = requests.post(
                f"{ollama_url}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
                timeout=timeout,
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "{}")
        # Clean markdown blocks
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        # Extract first JSON object
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            raw = m.group(0)
        return json.loads(raw)
    except Exception as e:
        print(f"    Ollama error: {e}")
        return {}


def _generate_one(session: dict, standings_txt: str, news_txt: str, ollama_url: str):
    prompt = f"""Tu es un expert F1 analytique avec 20 ans d'expérience.
Ton pronostic doit être PRÉCIS, basé sur les données ci-dessous.

═══ SESSION ═══
Type : {_session_description(session['session_type'])}
Course : {session['race_name']}
Circuit : {session['circuit']}
Date UTC : {session['session_date']}

═══ CLASSEMENT ACTUEL TOP 10 ═══
{standings_txt}

═══ ACTUALITÉS RÉCENTES (dernières 48h) ═══
{news_txt}

═══ CONSIGNES ═══
Pour cette session {session['session_type']} sur {session['circuit']}, génère ton top-3.
Utilise EXACTEMENT les driver_id Ergast (ex: "norris", "leclerc", "max_verstappen", "hamilton", "russell", "sainz").
Base ton raisonnement sur la forme récente et les actualités ci-dessus.

Réponds UNIQUEMENT avec ce JSON (rien d'autre, pas de texte autour) :
{{"p1":"driver_id","p2":"driver_id","p3":"driver_id","reasoning_fr":"Explication 80 mots en français","reasoning_en":"Explanation 80 words in English","confidence":"high|medium|low","key_factor_fr":"Facteur clé 10 mots"}}"""

    parsed = _call_ollama(prompt, ollama_url)
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


def generate_ai_predictions(data_dir: str, ollama_url: str) -> bool:
    print(f"\n=== AI PREDICTOR starting at {datetime.now()} ===")

    pred_file = os.path.join(data_dir, "ai_predictions.json")
    schedule = _load(os.path.join(data_dir, "current_schedule.json"))
    standings = _load(os.path.join(data_dir, "driver_standings.json"))
    news = _load(os.path.join(data_dir, "news.json")) or {"articles": []}

    if not schedule or not standings:
        print("  Missing schedule or standings — skipping")
        return False

    # Load existing predictions
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

    new_preds = dict(existing)
    generated = 0

    for session in upcoming:
        eid = session["event_id"]
        # Skip if prediction exists AND is less than 12h old
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
        pred = _generate_one(session, standings_txt, news_txt, ollama_url)
        if pred:
            new_preds[eid] = pred
            generated += 1
            print(f"    P1={pred['p1']} P2={pred['p2']} P3={pred['p3']} [{pred['confidence']}]")
            time.sleep(1)  # be nice to Ollama

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

    print(f"=== AI PREDICTOR done: {generated} predictions ===\n")
    return generated > 0


if __name__ == "__main__":
    import sys
    _ollama = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
    _data = os.environ.get("DATA_DIR", "data")
    if not os.path.exists(_data):
        os.makedirs(_data)
    generate_ai_predictions(_data, _ollama)
