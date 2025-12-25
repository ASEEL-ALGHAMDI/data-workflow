from pathlib import Path
import sys

# Locate project root directory
ROOT = Path(__file__).resolve().parents[1]

# Add /src to Python path so imports work correctly
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_workflow.reporting import build_final_report


def main() -> None:
    """Run Day 5 reporting pipeline."""
    
    # Input metrics file (generated in Day 4)
    metrics_path = ROOT / "data" / "processed" / "metrics_daily.parquet"
    
    # Output final formatted report
    out_path = ROOT / "reports" / "final_report.csv"

    print("🚀 Starting Day 5 reporting pipeline...")

    # Run and generate summary
    summary = build_final_report(metrics_path, out_path)

    print("📊 Final report generated at:", out_path)
    print("\n===== REPORT SUMMARY =====")
    print(summary)


if __name__ == "__main__":
    main()