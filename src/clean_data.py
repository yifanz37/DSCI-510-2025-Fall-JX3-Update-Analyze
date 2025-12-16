from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import re

RAW_MICRO = Path("data/raw/micro_list_raw.json")
RAW_XUNLEI = Path("data/raw/xunlei_list_raw.json")
OUT_CSV = Path("data/processed/updates_cleaned.csv")

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

def extract_records(raw: dict) -> list[dict]:
    records: list[dict] = []
    data = raw.get("data", [])

    for entry in data:
        if not isinstance(entry, dict):
            continue

        common_metadata = {
            "id": entry.get("id"),
            "name": entry.get("name") or entry.get("title"),
            "md5": entry.get("md5"),
            "version": entry.get("version") or entry.get("name"),
            "update_time": entry.get("update_time") or entry.get("time"),
            "game": entry.get("game"),
            "product_id": entry.get("product_id"),
            "size": entry.get("size"),
            "is_display": entry.get("is_display"),
            "downloadDir": entry.get("downloadDir"),
            "installFile": entry.get("installFile"),
            "excludePath": entry.get("excludePath"),
            "add_type": entry.get("add_type"),
        }

        urls = entry.get("url")
        if isinstance(urls, list):
            for download_url_item in urls:
                if isinstance(download_url_item, str):
                    record = common_metadata.copy()
                    record["download_url"] = download_url_item
                    records.append(record)
        elif isinstance(urls, str):
            record = common_metadata.copy()
            record["download_url"] = urls
            records.append(record)
        else:
            record = common_metadata.copy()
            record["download_url"] = None
            records.append(record)

    return records

def main() -> None:
    all_records: list[dict] = []

    if RAW_MICRO.exists():
        with open(RAW_MICRO, "r", encoding="utf-8") as f:
            micro_data = json.load(f)
            for rec in extract_records(micro_data):
                rec['source_type'] = 'micro'
                all_records.append(rec)

    if RAW_XUNLEI.exists():
        with open(RAW_XUNLEI, "r", encoding="utf-8") as f:
            xunlei_data = json.load(f)
            for rec in extract_records(xunlei_data):
                rec['source_type'] = 'xunlei'
                all_records.append(rec)

    df = pd.DataFrame(all_records)

    df['size_mb'] = df['size'].apply(size_to_mb)
    df = df.drop(columns=['size'])

    df["update_time"] = pd.to_datetime(df["update_time"], errors="coerce")
    df = df.dropna(subset=["update_time"]).copy()

    df["size_mb"] = pd.to_numeric(df["size_mb"], errors="coerce")
    df = df.dropna(subset=["version", "size_mb", "download_url"]).copy()

    key_cols = [c for c in ["id", "download_url", "update_time"] if c in df.columns]
    if key_cols:
        df = df.drop_duplicates(subset=key_cols)

    df = df.sort_values("update_time")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"Saved cleaned dataset -> {OUT_CSV}")
    print(f"Rows extracted: {len(df)}")
    print("Columns:", list(df.columns))


if __name__ == "__main__":
    main()
