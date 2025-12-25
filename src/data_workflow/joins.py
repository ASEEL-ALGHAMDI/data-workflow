from __future__ import annotations

import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: str | list[str],
    validate: str = "many_to_one",
    suffixes: tuple[str, str] = ("", "_r"),
) -> pd.DataFrame:
    """
    Safe left join using pandas merge with validation.

    validate="many_to_one" is perfect for: orders (many) -> users (one per user_id).
    It prevents join explosion if users has duplicate user_id.
    """
    return left.merge(
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )