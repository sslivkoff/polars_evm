from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl


def binary_to_float(
    df: pl.DataFrame,
    column: str,
    raw_type: str,
    replace: bool = False,
) -> pl.DataFrame:
    import polars as pl

    # decide hex column
    column_dtype = df.schema.get(column)
    if column_dtype == pl.Binary:
        hex_name = 'as_hex'
        hex_columns = {hex_name: pl.col(column).bin.encode("hex")}
        hex_expr = pl.col(hex_name)
    elif column_dtype == pl.String:
        hex_name = column
        hex_columns = {}
    else:
        raise Exception('invalid column dtype:' + str(column_dtype))

    expr = hex_expr_to_float(hex_expr, raw_type=raw_type)

    # rename
    if replace:
        expr = expr.alias(column)
    else:
        expr = expr.alias(column + '_f64')

    return (
        df.with_columns(**hex_columns)
        .with_columns(expr)
        .drop(*hex_columns.keys())
    )


# def hex_series_to_float(hex_series: pl.Series, raw_type: str) -> pl.Expr:
#     pl.DataFrame({'series': series})


def hex_expr_to_float(hex_expr: pl.Expr, raw_type: str) -> pl.Expr:
    import polars as pl

    # parse raw type
    raw_type = raw_type.lower()
    if raw_type.startswith("uint"):
        signed = False
        n_bits = int(raw_type[4:])
    elif raw_type.startswith("u"):
        signed = False
        n_bits = int(raw_type[1:])
    elif raw_type.startswith("int"):
        signed = True
        n_bits = int(raw_type[3:])
    elif raw_type.startswith("i"):
        signed = True
        n_bits = int(raw_type[1:])
    else:
        raise Exception()

    # build expression based on whether type is signed
    if signed:
        is_negative = hex_expr.str.slice(0, 2) > '7f'
        negative = _raw_hex_to_float(hex_expr, n_bits=n_bits, invert=True)
        positive = _raw_hex_to_float(hex_expr, n_bits=n_bits, invert=False)
        return pl.when(is_negative).then(negative).otherwise(positive)
    else:
        return _raw_hex_to_float(hex_expr, n_bits=n_bits, invert=False)


def _raw_hex_to_float(hex_expr: pl.Expr, *, n_bits: int, invert: bool) -> pl.Expr:
    if n_bits % 8 != 0:
        raise Exception('n_bits must be divisible by 8')
    n_remaining = n_bits
    exprs = []
    while n_remaining > 0:
        chunk_start = int((n_bits - n_remaining) / 8)
        chunk_size = min(8, n_remaining)
        expr = _float_chunk(
            chunk_start,
            chunk_size,
            n_bits,
            invert=invert,
            hex_expr=hex_expr,
        )
        exprs.append(expr)
        n_remaining -= 8 * chunk_size
    expr = sum(exprs)  # type: ignore
    if invert:
        expr = -expr
    return expr


def _float_chunk(
    start_byte: int,
    n_chunk_bytes: int,
    total_bits: int,
    *,
    hex_expr: pl.Expr,
    invert: bool = False,
) -> pl.Expr:
    import polars as pl

    if n_chunk_bytes > 8:
        raise Exception("n_chunk_bytes must be <= 8")

    expr = (
        hex_expr
        .str.slice(2 * start_byte, 2 * n_chunk_bytes)
        .str.decode("hex")
        .bin.reinterpret(dtype=pl.UInt64, endianness="big")
    )

    if invert:
        max_value = 2 ** int(8 * n_chunk_bytes) - 1
        expr = pl.lit(max_value, dtype=pl.UInt64) - expr

    factor = total_bits - 8.0 * (start_byte + n_chunk_bytes)
    expr = expr.cast(pl.Float64) * (2**factor)

    return expr

