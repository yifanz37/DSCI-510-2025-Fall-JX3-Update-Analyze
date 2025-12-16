from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import re

RAW_JSON_PATH = Path("data/raw/patch_packages_raw.json")
OUT_CSV_PATH = Path("data/processed/updates_cleaned.csv")

def size_to_mb(size_str: str) -> float | None:
    if not isinstance(size_str, str):
        return None
    
    size_str = size_str.strip().upper()
    match = re.match(r"(\d+\.?\d*)\s*([GM]B?)", size_str)
    if not match:
        return None
    
    value = float(match.group(1))
    unit = match.group(2)
    
    if unit == 'G' or unit == 'GB':
        return value * 1024
    elif unit == 'M' or unit == 'MB':
        return value
    return None

def main() -> None:
    with open(RAW_JSON_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    records = []

    for group in raw.get("data", []):
        common_metadata = {
            "id": group.get("id"),
            "name": group.get("name"),
            "md5": group.get("md5"),
            "size_mb": size_to_mb(group.get("size", "0B")),
            "version": group.get("version"),
            "update_time": group.get("update_time"),
            "game": group.get("game"),
            "product_id": group.get("product_id"),
            "is_display": group.get("is_display"),
            "downloadDir": group.get("downloadDir"),
            "installFile": group.get("installFile"),
            "excludePath": group.get("excludePath"),
            "add_type": group.get("add_type"),
        }

        for download_url_item in group.get("url", []):
            record = common_metadata.copy() # Start with the common metadata
            record["download_url"] = download_url_item # Add the specific download URL
            records.append(record)

    df = pd.DataFrame(records)

    df["update_time"] = pd.to_datetime(df["update_time"], errors="coerce")

    df["size_mb"] = pd.to_numeric(df["size_mb"], errors="coerce")
    df = df.dropna(subset=["version", "size_mb", "download_url"])

    OUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV_PATH, index=False, encoding="utf-8-sig")

    print(f"Saved cleaned dataset -> {OUT_CSV_PATH}")
    print(f"Rows extracted: {len(df)}")


if __name__ == "__main__":
    main()
