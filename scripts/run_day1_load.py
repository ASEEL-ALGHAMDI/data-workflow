import sys
import logging
from pathlib import Path

# -------------------------------------------------------------------
# Setup paths
# -------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# -------------------------------------------------------------------
# Imports (AFTER sys.path)
# -------------------------------------------------------------------
from data_workflow.config import make_paths
from data_workflow.io import read_orders_csv, read_users_csv, write_parquet
from data_workflow.transforms import enforce_schema
from data_workflow.quality import validate_orders

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main() -> None:
    logging.info("Starting Day 2 data load with quality checks")

    paths = make_paths(ROOT)

    # Load raw data
    orders = read_orders_csv(paths.raw / "orders.csv")
    users = read_users_csv(paths.raw / "users.csv")

    logging.info(f"Orders rows (raw): {len(orders)}")
    logging.info(f"Users rows (raw): {len(users)}")

    # Enforce schema
    orders = enforce_schema(orders)

    # Data quality checks
    orders = validate_orders(orders)

    logging.info(f"Orders rows (after quality): {len(orders)}")

    # Write outputs
    write_parquet(orders, paths.processed / "orders.parquet")
    write_parquet(users, paths.processed / "users.parquet")

    logging.info("Day 2 pipeline completed successfully")


if __name__ == "__main__":
    main()