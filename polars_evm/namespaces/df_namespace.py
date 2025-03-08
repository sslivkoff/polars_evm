from __future__ import annotations

import typing

import polars as pl

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

    def binary_to_float(
        self,
        column_types: dict[str, str],
        replace: bool = False,
    ) -> pl.DataFrame:
        return _helpers.binary_df_to_float(
            df=self._df, column_types=column_types, replace=replace
        )
