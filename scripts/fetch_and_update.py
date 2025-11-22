import os
import json
from datetime import datetime
import uuid

import feedparser

DATA_PATH = "data.json"

# RSS feeds to scan automatically
RSS_FEEDS = [
    "https://www.fooddive.com/rss/",
    "https://www.bevnet.com/feed",
    "https://www.nosh.com/feed",
    "https://www.prnewswire.com/rss/consumer-products-latest-news.rss",
    "https://www.globenewswire.com/RssFeed/subjectcode/8",
    "https://www.glossy.co/feed/",
    "https://www.beautymatter.com/feed",
    "https://www.marketingdive.com/feeds/news/",
    "https://adage.com/section/rss"
]

# Only keep articles on/after this date
CUTOFF_DATE_STR = "2025-11-01"
CUTOFF_DATE = datetime.fromisoformat(CUTOFF_DATE_STR)


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


def main():
    data = load_existing_data()
    existing_articles = data.get("articles", [])

    # Index existing by id so we don't duplicate
    id_to_article = {a.get("id"): a for a in existing_articles if a.get("id")}

    new_articles = []

    for feed_url in RSS_FEEDS:
        parsed = feedparser.parse(feed_url)

        for entry in parsed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            if not link:
                continue

            source = parsed.feed.get("title", "") or "Unknown"
            published_raw = entry.get("published", "") or entry.get("updated", "")

            published_dt = parse_date(published_raw)
            if not published_dt:
                continue

            # make timezone naive if needed
            if published_dt.tzinfo is not None:
                published_dt = published_dt.replace(tzinfo=None)

            if published_dt < CUTOFF_DATE:
                continue

            article_id = make_article_id(link, published_dt)
            if article_id in id_to_article:
                # already tracked
                continue

            summary = entry.get("summary", "") or entry.get("description", "") or ""

            article = {
                "id": article_id,
                "title": title,
                "url": link,
                "source": source,
                "published_at": published_dt.date().isoformat(),
                "summary": summary,
                "categories": [],
                "campaign_types": [],
                "demo_tags": [],
                "psych_tags": [],
                "stakeholders": [],
                "outreach_templates": []
            }

            id_to_article[article_id] = article
            new_articles.append(article)

    # Rebuild list, keep only articles after cutoff
    all_articles = list(id_to_article.values())
    filtered = []
    for art in all_articles:
        pd = parse_date(art.get("published_at", "1970-01-01"))
        if pd and pd >= CUTOFF_DATE:
            filtered.append(art)

    filtered.sort(
        key=lambda a: parse_date(a.get("published_at", "1970-01-01")) or CUTOFF_DATE,
        reverse=True,
    )

    data["articles"] = filtered
    save_data(data)

    print(f"Fetched {len(new_articles)} new article(s).")


if __name__ == "__main__":
    main()
