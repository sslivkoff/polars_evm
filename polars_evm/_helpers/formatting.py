from __future__ import annotations

import polars as pl


def set_column_display_width(width: int = 70) -> None:
    pl.Config.set_fmt_str_lengths(width)
