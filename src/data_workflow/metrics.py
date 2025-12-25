import pandas as pd


def build_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    metrics = {
        "total_orders": ("order_id", "count"),
        "total_revenue": ("amount_winsor", "sum"),
    }

    # handle outlier column name safely
    outlier_col = None
    if "amount_is_outlier" in df.columns:
        outlier_col = "amount_is_outlier"
    elif "amount__is_outlier" in df.columns:
        outlier_col = "amount__is_outlier"

    if outlier_col:
        metrics["outliers"] = (outlier_col, "sum")

    return (
        df.groupby("date")
        .agg(**metrics)
        .reset_index()
    )