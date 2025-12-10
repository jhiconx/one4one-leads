import os
import json
from datetime import datetime
import uuid

import feedparser
import requests
from textwrap import dedent

# ------------- CONFIG -------------

RSS_FEEDS = [
    "https://www.bevnet.com/feed",
    "https://www.bevnet.com/category/press-release/feed",
    "https://www.nosh.com/feed",
    "https://www.prnewswire.com/rss/consumer-products-latest-news.rss",
    "https://www.snackandbakery.com/rss",
    "https://www.glossy.co/feed/",
    "https://www.beautymatter.com/feed",
    "https://www.marketingdive.com/feeds/news/",
    "https://adage.com/section/rss"
]

# Keep only articles on/after this date
CUTOFF_DATE_STR = "2000-01-01"  # you can tighten later if you want
CUTOFF_DATE = datetime.fromisoformat(CUTOFF_DATE_STR)

DATA_PATH = "data.json"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise SystemExit("OPENAI_API_KEY not set in environment.")

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"  # [Speculation: ensure this matches your OpenAI endpoint]

# ------------- HELPERS -------------

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


def fetch_article_body(url):
    if not url:
        return ""
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.text
    except Exception:
        return ""


def call_openai_for_article(article_meta, article_text):
    """
    Ask the model to return a FULL article object in your schema.
    """

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

    Constraints:
    - Use the article metadata provided below for "title", "url", "source", and "published_at" when possible.
    - If a field is unknown, use an empty string "" or empty array [].
    - "categories" must be chosen from: ["food_and_beverage", "beauty_and_personal_care", "health_and_wellness", "other_cpg"].
    - "campaign_types" must be chosen from: ["product_launch", "sampling_program", "experiential_activation", "promotion_or_discount", "announcement", "other"].
    - "demo_tags" and "psych_tags" are short tokens like ["female", "gen_z_students", "college_students", "health_and_wellness_consumers"] etc.
    - For "stakeholders", include only people who appear to be marketing / brand / shopper / experiential / sampling decision-makers or clearly listed PR contacts.
    - Leave "email" blank "" if the article does not specify an email address.
    - Output MUST be valid JSON. Do not include any explanation or text outside the JSON.

    Article metadata:
    - Title: {article_meta.get("title", "")}
    - URL: {article_meta.get("link", "")}
    - Source: {article_meta.get("source", "")}
    - Published date: {article_meta.get("published", "")}

    Article text (may include HTML):
    {article_text}
    """)

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    body = {
        "model": "gpt-4.1-mini",  # [Speculation: replace with your actual model name if different]
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0
    }

    resp = requests.post(OPENAI_API_URL, headers=headers, json=body, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]

    # Clean up possible ```json fences
    content = content.strip()
    if content.startswith("```"):
        lines = content.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        content = "\n".join(lines).strip()

    # Extract JSON between first { and last }
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1:
        raise RuntimeError(f"Model output did not contain JSON braces: {content}")

    json_text = content[start:end + 1]

    try:
        return json.loads(json_text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse model output as JSON: {json_text}") from e


def main():
    data = load_existing_data()
    existing_articles = data.get("articles", [])

    # Index existing by id
    id_to_article = {a.get("id"): a for a in existing_articles if a.get("id")}

    new_articles = []

    for feed_url in RSS_FEEDS:
        print(f"Fetching feed: {feed_url}")
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

            # make timezone-naive if needed
            if published_dt.tzinfo is not None:
                published_dt = published_dt.replace(tzinfo=None)

            if published_dt < CUTOFF_DATE:
                continue

            article_id = make_article_id(link, published_dt)
            if article_id in id_to_article:
                # already processed
                continue

            article_meta = {
                "title": title,
                "link": link,
                "source": source,
                "published": published_dt.date().isoformat()
            }

            body_html = fetch_article_body(link)
            if not body_html:
                continue

            try:
                article_json = call_openai_for_article(article_meta, body_html)
            except Exception as e:
                print(f"Skipping article due to OpenAI error: {e}")
                continue

            # Ensure core fields are present / overridden by meta
            article_json.setdefault("id", article_id)
            article_json["id"] = article_id
            article_json.setdefault("url", link)
            article_json.setdefault("source", source)
            article_json.setdefault("published_at", published_dt.date().isoformat())
            article_json.setdefault("summary", "")
            article_json.setdefault("categories", [])
            article_json.setdefault("campaign_types", [])
            article_json.setdefault("demo_tags", [])
            article_json.setdefault("psych_tags", [])
            article_json.setdefault("stakeholders", [])
            article_json.setdefault("outreach_templates", [])

            id_to_article[article_json["id"]] = article_json
            new_articles.append(article_json)

    # Rebuild list, keep only articles on/after cutoff, normalize timezone here too
    all_articles = list(id_to_article.values())

    filtered = []
    for art in all_articles:
        pd = parse_date(art.get("published_at", ""))

        # Normalize timezone if present
        if pd and pd.tzinfo is not None:
            pd = pd.replace(tzinfo=None)

        if pd and pd >= CUTOFF_DATE:
            filtered.append(art)

    filtered.sort(key=lambda a: a.get("published_at", ""), reverse=True)

    data["articles"] = filtered
    save_data(data)

    print(f"Updated data.json with {len(new_articles)} new article(s).")


if __name__ == "__main__":
    main()
