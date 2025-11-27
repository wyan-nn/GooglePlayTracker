from google_play_scraper import app
from datetime import datetime, timezone
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_ID = "11Tyct_cEqn8syyuOmMbFbx9Rt2VZf7BZerUfwufknT8"

OVERVIEW_SHEET_NAME = "Current_Overview"
HISTORY_SHEET_NAME = "Overview_History"

SERVICE_ACCOUNT_FILE = "service_account.json"

APPS = [
    {"alias": "dtcpay",   "id": "com.dtc.wallet.app"},
    {"alias": "YouTrip",  "id": "co.you.youapp"},
    {"alias": "Wise",     "id": "com.transferwise.android"},
    {"alias": "Revolut",  "id": "com.revolut.revolut"},
    {"alias": "Redotpay", "id": "com.redotpay"},
]

FOCUS_MARKETS = ["sg", "my", "hk"]
APAC_MARKETS = [
    "sg", "my", "hk", "au", "nz", "jp", "kr", "tw", "cn",
    "in", "id", "th", "ph", "vn"
]
OTHER_MARKETS = [
    "us", "ca", "gb", "ie", "de", "fr", "it", "es", "nl", "se",
    "ch", "dk", "no", "fi",
    "br", "mx", "ar",
    "ae", "sa"
]

GLOBAL_MARKETS = sorted(set(APAC_MARKETS + OTHER_MARKETS))


def get_gsheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, scope
    )
    client = gspread.authorize(creds)
    return client


def fetch_play_info(app_id: str, country: str = "sg", lang: str = "en") -> dict:
    result = app(
        app_id,
        lang=lang,
        country=country
    )

    return {
        "title": result.get("title"),
        "country": country,
        "score": result.get("score"),
        "ratings_count": result.get("ratings"),
        "reviews_count": result.get("reviews"),
        "realInstalls": result.get("realInstalls")
                        or result.get("minInstalls")
                        or result.get("installs"),
        "version": result.get("version"),
        "lastUpdatedOn": str(result.get("lastUpdatedOn") or result.get("updated")),
    }


def update_play_sheets():
    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    try:
        overview_ws = sh.worksheet(OVERVIEW_SHEET_NAME)
    except gspread.WorksheetNotFound:
        overview_ws = sh.add_worksheet(OVERVIEW_SHEET_NAME, rows=1000, cols=20)

    try:
        history_ws = sh.worksheet(HISTORY_SHEET_NAME)
    except gspread.WorksheetNotFound:
        history_ws = sh.add_worksheet(HISTORY_SHEET_NAME, rows=1000, cols=20)

    header = [
        "query_time_iso",
        "app_alias",
        "country",
        "title",
        "score",
        "ratings_count",
        "reviews_count",
        "realInstalls",
        "version",
        "lastUpdatedOn",
        "is_apac",
        "is_focus",
    ]

    rows = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for app_info in APPS:
        alias = app_info["alias"]
        app_id = app_info["id"]

        for country in GLOBAL_MARKETS:
            info = fetch_play_info(app_id, country=country)

            is_apac = country in APAC_MARKETS
            is_focus = country in FOCUS_MARKETS

            rows.append([
                now_iso,
                alias,
                country,
                info["title"],
                info["score"],
                info["ratings_count"],
                info["reviews_count"],
                info["realInstalls"],
                info["version"],
                info["lastUpdatedOn"],
                is_apac,
                is_focus,
            ])

    overview_ws.clear()
    overview_ws.append_row(header)
    if rows:
        overview_ws.append_rows(rows)

    if len(history_ws.get_all_values()) == 0:
        history_ws.append_row(header)
    history_ws.append_rows(rows)

    print(f"Wrote {len(rows)} rows to Play_Overview and Play_History.")


if __name__ == "__main__":
    update_play_sheets()
