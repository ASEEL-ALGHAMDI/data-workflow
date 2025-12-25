from __future__ import annotations
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

def build_final_report(metrics_path: Path, out_path: Path):
    """Reads daily metrics file, creates summary report, and generates a chart."""

    # Load daily metrics data
    df = pd.read_parquet(metrics_path)

    # ---- Build summary table ----
    summary = pd.DataFrame({
        "total_days": [df["date"].nunique()],
        "total_orders": [df["total_orders"].sum()],
        "total_outliers": [df["total_outliers"].sum()],
        "total_revenue": [df["total_revenue"].sum()],
        "avg_revenue_per_day": [df["total_revenue"].mean()],
    })

    # ---- Save CSV report file ----
    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_path, index=False)
    print(f"[INFO] Saved report CSV --> {out_path}")

    # ---- Create plot (Daily Revenue Trend) ----
    plt.figure(figsize=(10, 6))
    plt.plot(df["date"], df["total_revenue"], marker="o", linewidth=2)
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.title("Daily Revenue Trend")

    figure_path = out_path.parent / "daily_revenue_plot.png"
    plt.savefig(figure_path, dpi=150, bbox_inches="tight")
    print(f"[INFO] Saved chart figure --> {figure_path}")

    plt.close()  # Close figure to avoid memory issues
    return summary


if __name__ == "__main__":
    build_final_report(
        Path("data/processed/metrics_daily.parquet"),
        Path("reports/final_report.csv")
    )