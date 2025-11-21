import os
import json
from datetime import datetime, timezone
import uuid

import feedparser
import requests

from textwrap import dedent

# ------------- CONFIG -------------

RSS_FEEDS = [
    "https://www.fooddive.com/rss/",
    "https://www.bevnet.com/feed",
    "https://www.nosh.com/feed",
    "https://www.prnewswire.com/rss/consumer-products-latest-news.rss",
    "https://www.globenewswire.com/RssFeed/subjectcode/8",
    "https://www.businesswire.com/portal/site/home/template.PAGE/rss/?javax.portlet.prp_9f56_80a4b7ac-13f2-4cfe-8e6a-93a0c180679d_viewID=MY_PORTLET_VIEW",
    "https://www.glossy.co/feed/",
    "https://www.beautymatter.com/feed",
    "https://www.marketingdive.com/feeds/news/",
    "https://adage.com/section/rss"
]


CUTOFF_DATE_STR = "2025-11-01"
CUTOFF_DATE = datetime.fromisoformat(CUTOFF_DATE_STR)

DATA_PATH = "data.json"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise SystemExit("OPENAI_API_KEY not set in environment.")

def load_existing_data():
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {"articles": []}
    else:
        return {"articles": []}

def save_data(data):
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except Exception:
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(date_str)
        except Exception:
            return None

def make_article_id(url, published_dt):
    base = (url or "") + (published_dt.isoformat() if published_dt else "")
    return "art_" + uuid.uuid5(uuid.NAMESPACE_URL, base).hex[:8]

import requests as rq

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

def call_openai_for_article(article_meta, article_text):
    prompt = dedent(f"""
    You are an information extraction engine for CPG launches and sampling campaigns.

    Output ONLY a JSON object with this exact structure:

    {{
      "id": string,
      "title": string,
      "url": string,
      "source": string,
      "published_at": string,
      "summary": string,
      "categories": string[],
      "campaign_types": string[],
      "demo_tags": string[],
      "psych_tags": string[],
      "stakeholders": [
        {{
          "full_name": string,
          "title": string,
          "company_name": string,
          "role_type": string,
          "linkedin_url": string,
          "email": string,
          "email_status": string,
          "email_confidence": number
        }}
      ],
      "outreach_templates": [
        {{
          "stakeholder_full_name": string,
          "email_subject": string,
          "email_body": string,
          "linkedin_message": string
        }}
      ]
    }}

    Article metadata:
    Title: {article_meta.get("title", "")}
    URL: {article_meta.get("link", "")}
    Source: {article_meta.get("source", "")}
    Published: {article_meta.get("published", "")}

    Article text:
    {article_text}
    """)

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    body = {
        "model": "gpt-4.1-mini",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0
    }

    resp = rq.post(OPENAI_API_URL, headers=headers, json=body, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse model output as JSON: {content}")

def fetch_article_body(url):
    if not url:
        return ""
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.text
    except Exception:
        return ""

def main():
    data = load_existing_data()
    existing_articles = data.get("articles", [])
    id_to_article = {a.get("id"): a for a in existing_articles if a.get("id")}

    new_articles = []

    for feed_url in RSS_FEEDS:
        parsed = feedparser.parse(feed_url)

        for entry in parsed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            source = parsed.feed.get("title", "") or "Unknown"
            published = entry.get("published", "") or entry.get("updated", "")

            published_dt = parse_date(published)
            if not published_dt:
                continue

            # Normalize timezone: make both dates naive

# Convert timezone-aware datetimes to naive datetimes

    # Convert timezone-aware datetimes to naive datetimes
if published_dt.tzinfo is not None:
    published_dt = published_dt.replace(tzinfo=None)
if published_dt < CUTOFF_DATE:
    continue

    continue


                continue

            article_id = make_article_id(link, published_dt)
            if article_id in id_to_article:
                continue

            article_meta = {
                "title": title,
                "link": link,
                "source": source,
                "published": published_dt.date().isoformat()
            }

            body_html = fetch_article_body(link)
            article_json = call_openai_for_article(article_meta, body_html)

            article_json.setdefault("id", article_id)
            article_json.setdefault("url", link)
            article_json.setdefault("source", source)
            article_json.setdefault("published_at", published_dt.date().isoformat())

            id_to_article[article_json["id"]] = article_json
            new_articles.append(article_json)

    all_articles = list(id_to_article.values())
    filtered = [
        art for art in all_articles
        if parse_date(art.get("published_at", "1970-01-01")) >= CUTOFF_DATE
    ]

    filtered.sort(
        key=lambda a: parse_date(a.get("published_at", "1970-01-01")),
        reverse=True
    )

    data["articles"] = filtered
    save_data(data)

if __name__ == "__main__":
    main()
