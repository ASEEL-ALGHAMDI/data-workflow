from __future__ import annotations

import pandas as pd


def parse_datetime(df: pd.DataFrame, col: str, utc: bool = True) -> pd.DataFrame:
    """
    Parse a datetime column safely. Invalid values become NaT.
    """
    ts = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: ts})


def add_time_parts(df: pd.DataFrame, ts_col: str = "created_at") -> pd.DataFrame:
    """
    Add common time features used for grouping/analytics.
    """
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )


def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """
    Compute IQR bounds for outlier detection.
    """
    x = s.dropna()
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """
    Clip extreme values for visualization (do NOT replace the raw amount for business logic).
    """
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)


def add_outlier_flag(df: pd.DataFrame, col: str, k: float = 1.5) -> pd.DataFrame:
    """
    Add a boolean column that flags outliers using IQR.
    """
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})