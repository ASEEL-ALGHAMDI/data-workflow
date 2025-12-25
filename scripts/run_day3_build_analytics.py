from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from data_workflow.config import make_paths
from data_workflow.joins import safe_left_join
from data_workflow.transforms import (
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("day3")


def load_orders_processed(processed_dir: Path) -> pd.DataFrame:
    """
    Prefer orders_clean.parquet if it exists, otherwise fall back to orders.parquet.
    """
    p_clean = processed_dir / "orders_clean.parquet"
    p_base = processed_dir / "orders.parquet"
    if p_clean.exists():
        log.info("Reading: %s", p_clean)
        return pd.read_parquet(p_clean)
    log.info("Reading: %s", p_base)
    return pd.read_parquet(p_base)


def main() -> None:
    paths = make_paths(ROOT)

    orders = load_orders_processed(paths.processed)
    users = pd.read_parquet(paths.processed / "users.parquet")

    log.info("orders rows: %d", len(orders))
    log.info("users rows: %d", len(users))

    # Parse datetime + add time features
    orders = orders.pipe(parse_datetime, col="created_at", utc=True).pipe(add_time_parts, ts_col="created_at")

    # Join protection: users should be unique on user_id
    if users["user_id"].duplicated().any():
        raise ValueError("users.user_id is not unique (would cause join explosion)")

    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # Ensure no row explosion
    if len(joined) != len(orders):
        raise ValueError("Row count changed after left join (join explosion?)")

    # Outlier helpers (keep original amount, add extra columns)
    if "amount" in joined.columns:
        joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
        joined = add_outlier_flag(joined, "amount", k=1.5)

    out_path = paths.processed / "analytics_table.parquet"
    joined.to_parquet(out_path, index=False)

    log.info("wrote: %s", out_path)
    log.info("Day 3 completed (true)")


if __name__ == "__main__":
    main()