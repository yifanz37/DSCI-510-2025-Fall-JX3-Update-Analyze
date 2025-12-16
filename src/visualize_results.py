from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

DATA_PATH = "data/processed/updates_cleaned.csv"
RESULTS_DIR = Path("results")

def main() -> None:
    RESULTS_DIR.mkdir(exist_ok=True)

    df = pd.read_csv(DATA_PATH, parse_dates=["update_time"])
    df = df.dropna(subset=["update_time"]).sort_values("update_time")

    df["month"] = df["update_time"].dt.to_period("M").dt.to_timestamp()
    monthly_count = df.groupby("month").size()

    plt.figure()
    plt.plot(monthly_count.index, monthly_count.values)
    plt.xlabel("Month")
    plt.ylabel("Number of Updates")
    plt.title("JX3 Update Frequency Over Time (Monthly Count)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "trend_monthly_update_count.png", dpi=200)
    plt.close()

    df["gap_days"] = df["update_time"].diff().dt.total_seconds() / 86400.0

    plt.figure()
    plt.plot(df["update_time"], df["gap_days"])
    plt.xlabel("Date")
    plt.ylabel("Days Since Previous Update")
    plt.title("JX3 Update Interval Trend (Gap Days)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "trend_update_gap_days.png", dpi=200)
    plt.close()

    print("Saved plots:")
    print(f"- {RESULTS_DIR / 'trend_monthly_update_count.png'}")
    print(f"- {RESULTS_DIR / 'trend_update_gap_days.png'}")

if __name__ == "__main__":
    main()
