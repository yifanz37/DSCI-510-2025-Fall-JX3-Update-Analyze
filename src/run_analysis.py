from __future__ import annotations

import pandas as pd

DATA_PATH = "data/processed/updates_cleaned.csv"


def main() -> None:
    df = pd.read_csv(DATA_PATH, parse_dates=["update_time"])
    df = df.dropna(subset=["update_time"]).sort_values("update_time")

    print("=== Basic Info ===")
    print("Rows:", len(df))
    print("Date range:", df["update_time"].min(), "to", df["update_time"].max())

    df["month"] = df["update_time"].dt.to_period("M").dt.to_timestamp()
    monthly_count = df.groupby("month").size()

    print("\n=== Monthly Update Frequency (summary) ===")
    print(monthly_count.describe())

    df["gap_days"] = df["update_time"].diff().dt.total_seconds() / 86400.0
    gaps = df["gap_days"].dropna()

    print("\n=== Update Interval in Days (summary) ===")
    print(gaps.describe())

    quiet = df[df["gap_days"] >= 14][["update_time", "gap_days"]].tail(10)
    print("\n=== Recent Quiet Periods (gap >= 14 days, last 10) ===")
    print(quiet.to_string(index=False) if len(quiet) else "None")


if __name__ == "__main__":
    main()
