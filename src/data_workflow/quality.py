import pandas as pd


def validate_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic data quality checks on orders.
    Invalid rows are removed.
    """

    initial_rows = len(df)

    df = df[
        df["order_id"].notna()
        & df["user_id"].notna()
        & df["amount"].notna()
        & (df["amount"] > 0)
    ]

    removed_rows = initial_rows - len(df)

    if removed_rows > 0:
        print(f"[QUALITY] Removed {removed_rows} invalid order rows")

    return df