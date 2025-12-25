from pathlib import Path
import sys
import logging
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_workflow.io import read_parquet, write_parquet
from data_workflow.metrics import build_daily_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    logging.info("Starting Day 4 metrics pipeline")

    analytics_path = ROOT / "data" / "processed" / "analytics_table.parquet"
    metrics_out = ROOT / "data" / "processed" / "metrics_daily.parquet"

    df = read_parquet(analytics_path)
    logging.info(f"Loaded analytics rows: {len(df)}")

    metrics = build_daily_metrics(df)
    write_parquet(metrics, metrics_out)

    logging.info(f"Wrote daily metrics to {metrics_out}")
    logging.info("Day 4 pipeline completed successfully")


if __name__ == "__main__":
    main()