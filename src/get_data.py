from __future__ import annotations

from pathlib import Path
import requests
import json

API_URL = "https://zt.xoyo.com/other/updatepage/index.php"
PARAMS = {
    "act": "apixunleilist",
    "game": "jx3V4",
    "num": 300
}

RAW_DIR = Path("data/raw")
RAW_JSON_PATH = RAW_DIR / "patch_packages_raw.json"


def fetch_json(url: str, params: dict) -> dict:
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    raw_text = resp.text.strip()
    if raw_text.startswith('?(') and raw_text.endswith(')'):
        json_string = raw_text[2:-1]
    else:
        json_string = raw_text

    print(f"Processed JSON string for parsing: {json_string}")
    return json.loads(json_string)


def save_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> None:
    data = fetch_json(API_URL, PARAMS)
    save_json(data, RAW_JSON_PATH)
    print(f"Saved raw JSON -> {RAW_JSON_PATH}")


if __name__ == "__main__":
    main()
