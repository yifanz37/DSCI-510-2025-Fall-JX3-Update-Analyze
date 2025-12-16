from __future__ import annotations

from pathlib import Path
import requests
import json

API_URL = "https://zt.xoyo.com/other/updatepage/index.php"

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


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
    params_xunlei = {"act": "apixunleilist", "game": "jx3V4", "num": 300}
    RAW_XUNLEI_PATH = RAW_DIR / "xunlei_list_raw.json"

    print("Fetching xunlei list...")
    xunlei_data = fetch_json(API_URL, params_xunlei)
    save_json(xunlei_data, RAW_XUNLEI_PATH)
    print(f"Saved raw JSON -> {RAW_XUNLEI_PATH}")

    params_micro = {"act": "apimicrolist", "game": "jx3V4", "num": 300}
    RAW_MICRO_PATH = RAW_DIR / "micro_list_raw.json"

    print("Fetching micro list...")
    micro_data = fetch_json(API_URL, params_micro)
    save_json(micro_data, RAW_MICRO_PATH)
    print(f"Saved raw JSON -> {RAW_MICRO_PATH}")


if __name__ == "__main__":
    main()
