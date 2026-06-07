#!/usr/bin/env python3
"""
F1 News Fetcher with Ollama AI Summaries (bilingual FR/EN).

Fetches F1 news from free RSS feeds, generates bilingual titles + summaries and a
0-100 importance score via Ollama, and writes data/news.json.

Improvements over the original draft:
  - Configurable model (OLLAMA_MODEL, default qwen2.5:7b — strong FR/EN).
  - Real bilingual TITLES (title_fr / title_en are translated, not copies).
  - Numeric importance score (0-100) + single primary `category` for app filters,
    `is_breaking` is derived from importance/category.
"""

import feedparser
import json
import hashlib
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
GROQ_URL = os.environ.get("GROQ_URL", "https://api.groq.com/openai/v1/chat/completions")
GROQ_MIN_INTERVAL = float(os.environ.get("GROQ_MIN_INTERVAL", "6.0"))  # sec between calls (free-tier RPM)
GROQ_MAX_RETRIES = int(os.environ.get("GROQ_MAX_RETRIES", "5"))
ACTIVE_MODEL = GROQ_MODEL if LLM_PROVIDER == "groq" else OLLAMA_MODEL

_GROQ_LAST_CALL = [0.0]  # module-level throttle state


def _groq_chat(prompt, temperature, timeout):
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
    resp.raise_for_status()  # exhausted retries -> raise the last 429
    return "{}"


def _llm_generate_json(prompt, temperature=0.15, top_p=0.9, timeout=120):
    """Return raw JSON-ish text from the configured LLM provider.

    The prompt must request JSON output (Groq JSON mode requires "json" in the
    messages, which the F1 prompts already satisfy via their JSON schema)."""
    if LLM_PROVIDER == "groq":
        return _groq_chat(prompt, temperature, timeout)
    resp = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={
            "model": OLLAMA_MODEL, "prompt": prompt, "stream": False,
            "format": "json", "options": {"temperature": temperature, "top_p": top_p},
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json().get("response", "{}")

NEWS_SOURCES = [
    {"url": "https://www.motorsport.com/rss/f1/news/", "source_name": "motorsport.com"},
    {"url": "https://www.autosport.com/rss/f1/news/", "source_name": "autosport.com"},
    {"url": "https://racefans.net/feed/", "source_name": "racefans.net"},
    {"url": "https://www.bbc.co.uk/sport/formula1/rss.xml", "source_name": "BBC Sport"},
    {"url": "https://news.google.com/rss/search?q=formule+1+F1+Grand+Prix&hl=fr&gl=FR&ceid=FR:fr", "source_name": "Google News FR"},
]

# Controlled vocabulary for the app's filter chips (single primary category per article).
CATEGORIES = [
    "résultat", "technique", "transfert", "pénalité", "accident",
    "météo", "stratégie", "rumeur", "réglementation", "breaking",
]

# Deterministic importance (0-100) per category. The LLM's own numeric score proved
# unreliable (it anchored everything at 50), so importance is derived from the
# category it picks, then boosted for breaking news. Keeps scores differentiated.
CATEGORY_IMPORTANCE = {
    "breaking": 92, "accident": 90, "pénalité": 72, "transfert": 68,
    "résultat": 60, "réglementation": 50, "technique": 48,
    "stratégie": 45, "rumeur": 42, "météo": 38,
}

F1_KEYWORDS = [
    "formula 1", "formula one", "f1", "grand prix", "formule 1",
    "verstappen", "norris", "leclerc", "hamilton", "sainz", "russell",
    "ferrari", "mclaren", "red bull", "mercedes", "alpine", "aston martin",
    "williams", "haas", "fia", "drs", "safety car",
]

MAX_ARTICLES = 40
MAX_AGE_HOURS = 48
MAX_FEED_ENTRIES = int(os.environ.get("MAX_FEED_ENTRIES", "15"))


def article_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def is_f1_related(entry) -> bool:
    text = f"{entry.get('title', '')} {entry.get('summary', '')}".lower()
    return any(kw in text for kw in F1_KEYWORDS)


def is_recent(entry) -> bool:
    published = entry.get("published_parsed")
    if not published:
        return True
    try:
        pub_dt = datetime(*published[:6], tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_AGE_HOURS)
        return pub_dt >= cutoff
    except Exception:
        return True


RACE_KEYWORDS = {
    "monaco": "monaco", "australian": "australia", "bahrain": "bahrain",
    "saudi": "saudi", "chinese": "china", "japanese": "japan",
    "miami": "miami", "canadian": "canada", "spanish": "spain-barcelona",
    "austrian": "austria", "british": "uk", "belgian": "belgium",
    "hungarian": "hungary", "dutch": "netherlands", "italian": "italy",
    "madrid": "spain-madrid", "azerbaijani": "azerbaijan", "baku": "azerbaijan",
    "singapore": "singapore", "united states": "usa", "austin": "usa",
    "mexican": "mexico", "brazilian": "brazil", "sao paulo": "brazil",
    "las vegas": "lasvegas", "qatar": "qatar", "abu dhabi": "abudhabi",
}


def guess_race_id(text: str):
    text_lower = text.lower()
    for keyword, race_id in RACE_KEYWORDS.items():
        if keyword in text_lower:
            return race_id
    return None


_IMG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)

# og:image / twitter:image meta tag — attribute order varies between sites.
_OG_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?:og:image(?::url)?|twitter:image(?::src)?)["\'][^>]+content=["\']([^"\']+)["\']'
    r'|<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:og:image(?::url)?|twitter:image(?::src)?)["\']',
    re.IGNORECASE,
)

_OG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
}


def _normalize_img(url):
    if not url:
        return None
    url = url.strip()
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("http"):
        return url
    return None


def fetch_og_image(url):
    """Fetch the article page and pull its og:image / twitter:image. Best-effort."""
    if not url or not url.startswith("http"):
        return None
    try:
        r = requests.get(url, timeout=8, headers=_OG_HEADERS, allow_redirects=True)
        if r.status_code != 200:
            return None
        html = r.text[:250000]  # meta tags live in <head>, no need to scan the whole page
        m = _OG_RE.search(html)
        if m:
            return _normalize_img(m.group(1) or m.group(2))
    except Exception:
        return None
    return None


def extract_image(entry):
    """Best-effort thumbnail extraction across the many RSS image conventions,
    falling back to the article page's og:image when the feed carries none."""
    # 1. media:thumbnail / media:content (Yahoo Media RSS)
    for attr in ("media_thumbnail", "media_content"):
        media = getattr(entry, attr, None)
        if media:
            for m in media:
                if m.get("url"):
                    return _normalize_img(m["url"])
    # 2. enclosures / links flagged as images
    for link in (entry.get("links") or []) + (entry.get("enclosures") or []):
        if "image" in (link.get("type") or "") and link.get("href"):
            return _normalize_img(link["href"])
        if link.get("rel") == "enclosure" and "image" in (link.get("type") or ""):
            return _normalize_img(link.get("href"))
    # 3. first <img> inside the summary/content HTML
    html = entry.get("summary", "") or ""
    if not html and entry.get("content"):
        try:
            html = entry["content"][0].get("value", "")
        except Exception:
            html = ""
    m = _IMG_RE.search(html)
    if m:
        return _normalize_img(m.group(1))
    # 4. Fallback: scrape og:image from the article page (covers Google News, BBC…)
    return fetch_og_image(entry.get("link"))


def _clean_json(raw: str) -> str:
    """Strip markdown fences and isolate the first JSON object."""
    if "```" in raw:
        raw = raw.split("```")[1].replace("json", "").strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        raw = raw[start:end + 1]
    return raw


def analyze_with_ollama(title: str, description: str) -> dict:
    """Ask Ollama for bilingual title+summary, category, importance (0-100)."""
    content = f"Titre original: {title}\n\nContenu: {description[:800]}"
    prompt = (
        "Tu es journaliste F1 pour un média FRANÇAIS. Tu écris dans un français "
        "journalistique naturel et fluide, comme L'Équipe ou Canal+.\n"
        "Réponds UNIQUEMENT en JSON valide, sans aucun texte autour.\n\n"
        "RÈGLES DE RÉDACTION FRANÇAISE (champs _fr) — STRICTES :\n"
        "- Rédige DIRECTEMENT en français. Ne traduis PAS l'anglais mot à mot : "
        "reformule comme un journaliste français l'écrirait spontanément.\n"
        "- Capitalisation française NORMALE : majuscule en début de phrase et aux noms "
        "propres uniquement. INTERDIT le Title Case anglais "
        "(ex. écris « Alonso impressionne à Monaco », PAS « Alonso Impressionne à Monaco »).\n"
        "- Noms propres EXACTS : Monaco (pas « Monacos »), Verstappen, Aston Martin, "
        "Red Bull, McLaren. Ne déforme jamais un nom.\n"
        "- Ordre des mots correct : « le pilote Aston Martin » (PAS « l'Aston Martin pilote »).\n"
        "- Zéro mot anglais dans les champs _fr, zéro caractère non latin.\n"
        "- summary_fr : EXACTEMENT 1 à 2 phrases, 40 mots maximum, factuel.\n"
        "- title_fr : court, accrocheur, factuel, 1 ligne.\n"
        "Les champs _en sont rédigés à 100% en anglais naturel (mêmes règles de concision).\n\n"
        "EXEMPLE (style attendu) :\n"
        'Entrée : "Fernando Alonso arrives at Monaco in a rare Porsche 918 Spyder, '
        'talks Aston Martin upgrades coming next race."\n'
        'Sortie : {"title_fr":"Alonso débarque à Monaco au volant d\'une Porsche 918",'
        '"title_en":"Alonso arrives at Monaco in a Porsche 918",'
        '"summary_fr":"Fernando Alonso a fait sensation à Monaco en arrivant dans une rare '
        'Porsche 918 Spyder. Le pilote Aston Martin attend des évolutions dès la prochaine course.",'
        '"summary_en":"Fernando Alonso turned heads at Monaco arriving in a rare Porsche 918 Spyder. '
        'The Aston Martin driver expects upgrades as soon as the next race.",'
        '"category":"rumeur","is_breaking":false}\n\n'
        f"ARTICLE À TRAITER :\n{content}\n\n"
        "Génère le JSON avec ces champs :\n"
        "- title_fr, title_en, summary_fr, summary_en (selon les règles ci-dessus)\n"
        f"- category : UNE seule valeur EXACTE parmi : {CATEGORIES}\n"
        "- is_breaking : true UNIQUEMENT si incident grave / disqualification / accident / "
        "annulation / annonce majeure de dernière minute, sinon false\n\n"
        '{"title_fr":"...","title_en":"...","summary_fr":"...","summary_en":"...",'
        '"category":"...","is_breaking":false}'
    )
    try:
        parsed = json.loads(_clean_json(_llm_generate_json(prompt, temperature=0.15, top_p=0.9)))

        category = str(parsed.get("category", "")).strip().lower()
        if category not in CATEGORIES:
            category = "rumeur"
        is_breaking = bool(parsed.get("is_breaking", False))

        # Deterministic importance from category, boosted for breaking news.
        importance = CATEGORY_IMPORTANCE.get(category, 42)
        if is_breaking:
            importance = max(importance, 88)

        return {
            "title_fr": str(parsed.get("title_fr", title))[:200] or title,
            "title_en": str(parsed.get("title_en", title))[:200] or title,
            "summary_fr": str(parsed.get("summary_fr", ""))[:500],
            "summary_en": str(parsed.get("summary_en", ""))[:500],
            "category": category,
            "importance": importance,
            "is_breaking": is_breaking,
        }
    except Exception as e:
        print(f"    Ollama analyze error: {e}")
        fallback = (description or title)[:200]
        return {
            "title_fr": title, "title_en": title,
            "summary_fr": fallback, "summary_en": fallback,
            "category": "rumeur", "importance": 30, "is_breaking": False,
        }


def fetch_and_process_news(data_dir: str) -> bool:
    print(f"\n=== NEWS FETCH starting at {datetime.now()} (provider={LLM_PROVIDER}, model={ACTIVE_MODEL}) ===")
    news_file = os.path.join(data_dir, "news.json")

    existing = {}
    if os.path.exists(news_file):
        try:
            with open(news_file) as f:
                old = json.load(f)
                existing = {a["id"]: a for a in old.get("articles", [])}
        except Exception:
            pass

    new_count = 0
    all_articles = list(existing.values())

    for source in NEWS_SOURCES:
        print(f"  Fetching {source['source_name']}...")
        try:
            feed = feedparser.parse(source["url"])
            entries = feed.entries[:MAX_FEED_ENTRIES]
        except Exception as e:
            print(f"    Feed error: {e}")
            continue

        for entry in entries:
            if not is_recent(entry) or not is_f1_related(entry):
                continue
            url = entry.get("link", "")
            if not url:
                continue
            art_id = article_id(url)
            if art_id in existing:
                # Backfill a missing image for an already-known article (mutates in place).
                if not existing[art_id].get("image_url"):
                    img = extract_image(entry)
                    if img:
                        existing[art_id]["image_url"] = img
                continue

            title = entry.get("title", "")
            description = entry.get("summary", entry.get("description", ""))
            print(f"    New article: {title[:60]}...")

            image_url = extract_image(entry)

            published_at = datetime.now(timezone.utc).isoformat()
            if entry.get("published_parsed"):
                try:
                    dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    published_at = dt.isoformat()
                except Exception:
                    pass

            ai = analyze_with_ollama(title, description)
            time.sleep(0.3)

            article = {
                "id": art_id,
                "title": title,                 # original (source language) kept for reference
                "title_fr": ai["title_fr"],
                "title_en": ai["title_en"],
                "source": source["source_name"],
                "url": url,
                "image_url": image_url,
                "published_at": published_at,
                "race_id": guess_race_id(f"{title} {description}"),
                "category": ai["category"],
                "tags": [ai["category"]],
                "importance": ai["importance"],
                "is_breaking": ai["is_breaking"],
                "summary_fr": ai["summary_fr"],
                "summary_en": ai["summary_en"],
            }
            all_articles.append(article)
            existing[art_id] = article
            new_count += 1

    all_articles.sort(key=lambda a: a.get("published_at", ""), reverse=True)
    all_articles = all_articles[:MAX_ARTICLES]

    output = {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "model": ACTIVE_MODEL,
        "articles": all_articles,
    }
    with open(news_file, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"=== NEWS FETCH done: {new_count} new, {len(all_articles)} total ===\n")
    return new_count > 0


if __name__ == "__main__":
    _data = os.environ.get("DATA_DIR", "data")
    if not os.path.exists(_data):
        os.makedirs(_data)
    fetch_and_process_news(_data)
