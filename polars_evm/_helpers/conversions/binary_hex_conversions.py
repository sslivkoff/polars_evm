from __future__ import annotations

import typing

import polars as pl

if typing.TYPE_CHECKING:
    _T = typing.TypeVar('_T', pl.DataFrame, pl.LazyFrame)


#
# # binary to hex
#


def binary_df_to_hex(
    df: _T,
    columns: typing.Sequence[str] | None = None,
    *,
    prefix: bool = True,
) -> _T:
    import polars as pl

    if columns is None:
        columns = [
            column
            for column, dtype in df.collect_schema().items()
            if dtype == pl.Binary
        ]

    exprs = [
        binary_expr_to_hex(pl.col(column), prefix=prefix) for column in columns
    ]
    return df.with_columns(exprs)


def binary_series_to_hex(series: pl.Series, prefix: bool = True) -> pl.Series:
    series = series.bin.encode('hex')
    if prefix:
        series = ('0x' + series).rename(series.name)
    return series


def binary_expr_to_hex(expr: pl.Expr, prefix: bool = True) -> pl.Expr:
    expr = expr.bin.encode('hex')
    if prefix:
        old_name = expr.meta.output_name()
        expr = '0x' + expr
        if old_name is not None:
            expr = expr.alias(old_name)
    return expr


#
# # hex to binary
#


def hex_df_to_binary(
    df: _T,
    columns: list[str] | None = None,
    *,
    prefix: bool | None = None,
) -> _T:
    import polars as pl

    if columns is None:
        columns = [
            column
            for column, dtype in df.collect_schema().items()
            if dtype == pl.String
        ]

    exprs = [
        hex_expr_to_binary(pl.col(column), prefix=prefix) for column in columns
    ]
    return df.with_columns(exprs)


def hex_series_to_binary(
    series: pl.Series, prefix: bool | None = None
) -> pl.Series:
    if prefix is None or (isinstance(prefix, bool) and prefix):
        series = series.str.strip_prefix('0x')
    return series.str.decode('hex')


def hex_expr_to_binary(expr: pl.Expr, prefix: bool | None = None) -> pl.Expr:
    if prefix is None or (isinstance(prefix, bool) and prefix):
        expr = expr.str.strip_prefix('0x')
    return expr.str.decode('hex')
