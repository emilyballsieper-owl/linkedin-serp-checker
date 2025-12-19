import os
import datetime
import requests
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
SEARCH_ENGINE_ID = os.environ["SEARCH_ENGINE_ID"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"]

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Sheet1"


def extract_username(url):
    return url.rstrip("/").split("/in/")[-1]


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

    for r in data.get("items", []):
        link = r.get("link", "").lower()
        snippet = r.get("snippet", "").lower()

        if "/pub/dir/" in link:
            return "NOT_REAL", -80, "directory"

        if f"/in/{username}" in link:
            if " at " in snippet or " · " in snippet:
                return "REAL", 80, "profile_snippet"
            return "LIKELY_REAL", 40, "exact_match"

    return "UNCERTAIN", 10, "weak_signal"


def write_to_sheet(rows):
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:F",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()


def main():
    urls = [
        # TEMP test values — we will replace this next
        "https://linkedin.com/in/example-profile"
    ]

    rows = []

    for url in urls:
        username = extract_username(url)
        data = google_search(f'"linkedin.com/in/{username}"')
        status, score, signal = classify_result(data, username)

        rows.append([
            url,
            username,
            status,
            score,
            signal,
            datetime.datetime.utcnow().isoformat()
        ])

    write_to_sheet(rows)


if __name__ == "__main__":
    main()
