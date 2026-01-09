import os
import json
import time
import requests
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

NOTION_VERSION = "2022-06-28"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# === 你的 Notion 欄位名稱（如有不同，改這裡即可） ===
PROP_TITLE = "Title"
PROP_ARXIV_ID = "arXiv ID"
PROP_PDF_URL = "pdf_url"
PROP_CODE_URL = "code_url"
PROP_ABSTRACT = "Abstract"
PROP_AUTHORS = "Authors"
PROP_CATEGORY = "Category"
PROP_SCORE = "Relevance Score"
PROP_DATE_ADDED = "Date Added"
PROP_SOURCE = "Source"
PROP_STATUS = "Status"

DEFAULT_SOURCE = "arxiv-daily"
DEFAULT_STATUS = "To Read"

RECO_JSON_PATH = os.environ.get("RECOMMENDATIONS_JSON", "output/recommendations.json")


def notion_query_by_arxiv_id(arxiv_id: str) -> str | None:
    """Return page_id if exists."""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": PROP_ARXIV_ID,
            "rich_text": {"equals": arxiv_id},
        },
        "page_size": 1,
    }
    r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    if not results:
        return None
    return results[0]["id"]


def notion_create_page(p: dict):
    url = "https://api.notion.com/v1/pages"
    now_iso = datetime.now(timezone.utc).date().isoformat()

    props = {
        PROP_TITLE: {"title": [{"text": {"content": p["title"][:2000]}}]},
        PROP_ARXIV_ID: {"rich_text": [{"text": {"content": p["arxiv_id"]}}]},
        PROP_PDF_URL: {"url": p.get("pdf_url") or None},
        PROP_CODE_URL: {"url": p.get("code_url") or None},
        PROP_ABSTRACT: {"rich_text": [{"text": {"content": (p.get("abstract") or "")[:2000]}}]},
        PROP_AUTHORS: {"rich_text": [{"text": {"content": (p.get("authors") or "")[:2000]}}]},
        PROP_SCORE: {"number": p.get("score")},
        PROP_DATE_ADDED: {"date": {"start": now_iso}},
        PROP_SOURCE: {"select": {"name": DEFAULT_SOURCE}},
        PROP_STATUS: {"select": {"name": DEFAULT_STATUS}},
    }

    cats = p.get("category") or []
    if isinstance(cats, str):
        cats = [cats]
    cats = [c.strip() for c in cats if c and c.strip()]
    if cats:
        props[PROP_CATEGORY] = {"multi_select": [{"name": c[:100]} for c in cats[:10]]}

    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": props,
    }

    r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    if not r.ok:
        print("Notion create page failed")
        print("Status:", r.status_code)
        print("Response:", r.text)

    r.raise_for_status()
    return r.json()["id"]


def notion_update_page(page_id: str, p: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    now_iso = datetime.now(timezone.utc).date().isoformat()

    # 嚴格只更新「機器欄位」：不要覆蓋你的人類筆記欄位
    props = {
        PROP_SCORE: {"number": p.get("score")},
        PROP_DATE_ADDED: {"date": {"start": now_iso}},
    }

    r = requests.patch(url, headers=HEADERS, json={"properties": props}, timeout=30)
    r.raise_for_status()


def main():
    with open(RECO_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    papers = data.get("papers", [])
    if not papers:
        print("No papers in recommendations.json; skip Notion upsert.")
        return

    created = 0
    updated = 0

    for p in papers:
        arxiv_id = (p.get("arxiv_id") or "").strip()
        title = (p.get("title") or "").strip()
        if not arxiv_id or not title:
            continue

        page_id = notion_query_by_arxiv_id(arxiv_id)
        if page_id is None:
            notion_create_page(p)
            created += 1
        else:
            notion_update_page(page_id, p)
            updated += 1

        # Notion rate limit 保守一點（避免偶發 429）
        time.sleep(0.35)

    print(f"Notion upsert done. created={created}, updated={updated}, total={len(papers)}")


if __name__ == "__main__":
    main()
