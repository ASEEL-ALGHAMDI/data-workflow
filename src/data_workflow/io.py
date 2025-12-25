from pathlib import Path
import pandas as pd

# Centralized definition for missing values
NA_VALUES = ["", "NA", "N/A", "null", "None"]


def read_orders_csv(path: Path) -> pd.DataFrame:
    """
    Read orders CSV with proper dtypes and NA handling.
    """
    return pd.read_csv(
        path,
        dtype={
            "order_id": "string",
            "user_id": "string",
        },
        na_values=NA_VALUES,
        keep_default_na=True,
    )


def read_users_csv(path: Path) -> pd.DataFrame:
    """
    Read users CSV with proper dtypes and NA handling.
    """
    return pd.read_csv(
        path,
        dtype={
            "user_id": "string",
        },
        na_values=NA_VALUES,
        keep_default_na=True,
    )


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """
    Write DataFrame to Parquet, creating parent directories if needed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def read_parquet(path: Path) -> pd.DataFrame:
    """
    Read a Parquet file into a DataFrame.
    """
    return pd.read_parquet(path)