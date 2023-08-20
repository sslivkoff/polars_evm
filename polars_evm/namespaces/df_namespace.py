from __future__ import annotations

import polars as pl

import typing

from .. import _helpers


@pl.api.register_dataframe_namespace('evm')
class DataFrameEvm:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def binary_to_hex(
        self, columns: typing.Sequence[str] | None = None, prefix: bool = True
    ) -> pl.DataFrame:
        if prefix:
            return _helpers.binary_columns_to_prefix_hex(
                self._df, columns=columns
            )
        else:
            return _helpers.binary_columns_to_raw_hex(self._df, columns=columns)

    def hex_to_binary(
        self, columns: typing.Sequence[str] | None = None, prefix: bool = True
    ) -> pl.DataFrame:
        if prefix:
            return _helpers.prefix_hex_columns_to_binary(
                self._df, columns=columns
            )
        else:
            return _helpers.raw_hex_columns_to_binary(self._df, columns=columns)

