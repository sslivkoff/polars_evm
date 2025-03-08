from __future__ import annotations

import typing

import polars as pl

from .. import _helpers


@pl.api.register_lazyframe_namespace('evm')
class LazyFrameEvm:
    def __init__(self, lf: pl.LazyFrame):
        self._lf = lf

    def binary_to_hex(
        self, columns: typing.Sequence[str] | None = None, prefix: bool = True
    ) -> pl.LazyFrame:
        return _helpers.binary_df_to_hex(
            self._lf, columns=columns, prefix=prefix
        )

    def hex_to_binary(
        self, columns: typing.Sequence[str] | None = None, prefix: bool = True
    ) -> pl.LazyFrame:
        return _helpers.binary_df_to_hex(
            self._lf, columns=columns, prefix=prefix
        )

    def binary_to_float(
        self,
        column_types: dict[str, str],
        replace: bool = False,
    ) -> pl.LazyFrame:
        return _helpers.binary_df_to_float(
            df=self._lf, column_types=column_types, replace=replace
        )
