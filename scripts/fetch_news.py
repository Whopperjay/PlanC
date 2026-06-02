#!/usr/bin/env python3
"""
F1 News Fetcher with Ollama AI Summaries
Fetches F1 news from RSS feeds, generates bilingual summaries via Ollama,
and saves to data/news.json.
"""

import feedparser
import json
import hashlib
import os
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


NEWS_SOURCES = [
    {"url": "https://www.motorsport.com/rss/f1/news/", "source_name": "motorsport.com"},
    {"url": "https://www.autosport.com/rss/f1/news/", "source_name": "autosport.com"},
    {"url": "https://racefans.net/feed/", "source_name": "racefans.net"},
    {"url": "https://www.bbc.co.uk/sport/formula1/rss.xml", "source_name": "BBC Sport"},
    {"url": "https://news.google.com/rss/search?q=formule+1+F1+Grand+Prix&hl=fr&gl=FR&ceid=FR:fr", "source_name": "Google News FR"},
]

F1_KEYWORDS = [
    "formula 1", "formula one", "f1", "grand prix", "formule 1",
    "verstappen", "norris", "leclerc", "hamilton", "sainz", "russell",
    "ferrari", "mclaren", "red bull", "mercedes", "alpine", "aston martin",
    "williams", "haas", "fia", "drs", "safety car",
]

MAX_ARTICLES = 40
MAX_AGE_HOURS = 48


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


def summarize_with_ollama(title: str, description: str, ollama_url: str) -> dict:
    content = f"Titre: {title}\n\nContenu: {description[:800]}"
    prompt = (
        "Tu es un journaliste F1 expert et concis. Analyse cet article et réponds "
        "UNIQUEMENT en JSON valide sans texte autour.\n\n"
        f"Article:\n{content}\n\n"
        "Génère:\n"
        "1. summary_fr: résumé factuel 80 mots max en français\n"
        "2. summary_en: même résumé 80 mots max en anglais\n"
        "3. tags: liste parmi [résultat, technique, transfert, pénalité, accident, météo, breaking, stratégie]\n"
        "4. is_breaking: true si incident grave / disqualification / accident / annulation\n\n"
        'Format: {"summary_fr":"...","summary_en":"...","tags":["..."],"is_breaking":false}'
    )

    try:
        if LLM_PROVIDER == "groq":
            raw = _groq_chat(prompt, timeout=90)
        else:
            resp = requests.post(
                f"{ollama_url}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
                timeout=90,
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "{}")
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        parsed = json.loads(raw)
        return {
            "summary_fr": str(parsed.get("summary_fr", ""))[:500],
            "summary_en": str(parsed.get("summary_en", ""))[:500],
            "tags": [str(t) for t in parsed.get("tags", [])][:5],
            "is_breaking": bool(parsed.get("is_breaking", False)),
        }
    except Exception as e:
        print(f"    Ollama summarize error: {e}")
        fallback = (description or title)[:200]
        return {
            "summary_fr": fallback,
            "summary_en": fallback,
            "tags": [],
            "is_breaking": False,
        }


def fetch_and_process_news(data_dir: str, ollama_url: str) -> bool:
    print(f"\n=== NEWS FETCH starting at {datetime.now()} ===")
    news_file = os.path.join(data_dir, "news.json")

    # Load existing articles
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
            entries = feed.entries[:15]
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
                continue

            title = entry.get("title", "")
            description = entry.get("summary", entry.get("description", ""))
            print(f"    New article: {title[:60]}...")

            # Image
            image_url = None
            for attr in ["media_thumbnail", "media_content"]:
                media = getattr(entry, attr, None)
                if media:
                    image_url = media[0].get("url")
                    break

            # Date
            published_at = datetime.now(timezone.utc).isoformat()
            if entry.get("published_parsed"):
                try:
                    dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    published_at = dt.isoformat()
                except Exception:
                    pass

            ai = summarize_with_ollama(title, description, ollama_url)
            time.sleep(0.5)

            article = {
                "id": art_id,
                "title": title,
                "title_fr": title,
                "title_en": title,
                "source": source["source_name"],
                "url": url,
                "image_url": image_url,
                "published_at": published_at,
                "race_id": guess_race_id(f"{title} {description}"),
                "tags": ai["tags"],
                "is_breaking": ai["is_breaking"],
                "summary_fr": ai["summary_fr"],
                "summary_en": ai["summary_en"],
            }
            all_articles.append(article)
            existing[art_id] = article
            new_count += 1

    # Sort newest first, keep max
    all_articles.sort(key=lambda a: a.get("published_at", ""), reverse=True)
    all_articles = all_articles[:MAX_ARTICLES]

    output = {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "articles": all_articles,
    }
    with open(news_file, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"=== NEWS FETCH done: {new_count} new, {len(all_articles)} total ===\n")
    return new_count > 0


if __name__ == "__main__":
    _ollama = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
    _data = os.environ.get("DATA_DIR", "data")
    if not os.path.exists(_data):
        os.makedirs(_data)
    fetch_and_process_news(_data, _ollama)
