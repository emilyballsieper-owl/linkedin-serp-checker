import os
import json
import datetime
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===== ENV =====
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
SEARCH_ENGINE_ID = os.environ["SEARCH_ENGINE_ID"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Sheet1"

BATCH_SIZE = 100

# ===== HELPERS =====

def extract_username(url):
    if not url:
        return ""
    url = url.rstrip("/")
    if "/in/" in url:
        return url.split("/in/")[-1]
    return ""

def google_search(query):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": SEARCH_ENGINE_ID,
        "q": query,
        "num": 5
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def classify_result(data, username):
    raw = json.dumps(data).lower()

    if "missing:" in raw:
        return "NOT_REAL", -100, "missing"

    for item in data.get("items", []):
        link = item.get("link", "").lower()
        snippet = item.get("snippet", "").lower()

        if "/pub/dir/" in link:
            return "NOT_REAL", -80, "directory"

        if f"/in/{username}" in link:
            if " at " in snippet or " · " in snippet or "connections" in snippet:
                return "REAL", 80, "profile_snippet"
            return "LIKELY_REAL", 40, "exact_match"

    return "UNCERTAIN", 10, "weak_signal"

def sheets_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

# ===== MAIN =====

def main():
    service = sheets_service()
    sheet = service.spreadsheets()

    # Read all rows
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A2:I"
    ).execute()

    rows = result.get("values", [])
    updates = []
    processed_count = 0
    today = datetime.date.today().isoformat()
    now = datetime.datetime.utcnow().isoformat()

    for idx, row in enumerate(rows, start=2):
        if processed_count >= BATCH_SIZE:
            break

        person_id = row[0] if len(row) > 0 else ""
        linkedin_url = row[1] if len(row) > 1 else ""
        processed = row[7] if len(row) > 7 else ""

        if not person_id or not linkedin_url:
            continue

        if str(processed).lower() == "true":
            continue

        username = extract_username(linkedin_url)
        data = google_search(f'"linkedin.com/in/{username}"')
        status, score, signals = classify_result(data, username)

        updates.append({
            "range": f"{SHEET_NAME}!C{idx}:I{idx}",
            "values": [[
                username,
                status,
                score,
                signals,
                now,
                "TRUE",
                today
            ]]
        })

        processed_count += 1

    if updates:
        sheet.values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "valueInputOption": "RAW",
                "data": updates
            }
        ).execute()

if __name__ == "__main__":
    main()
