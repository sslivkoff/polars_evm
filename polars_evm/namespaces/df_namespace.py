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
        return _helpers.binary_df_to_hex(
            self._df, columns=columns, prefix=prefix
        )

    def hex_to_binary(
        self, columns: typing.Sequence[str] | None = None, prefix: bool = True
    ) -> pl.DataFrame:
        return _helpers.binary_df_to_hex(
            self._df, columns=columns, prefix=prefix
        )

    def binary_to_float(
        self, column_types: dict[str, str], replace: bool = False
    ) -> pl.DataFrame:
        return _helpers.binary_df_to_float(
            df=self._df, column_types=column_types, replace=replace
        )

    def decode_events(
        self,
        event_abi: dict[str, typing.Any],
        *,
        columns: list[str] | None = None,
        drop_raw_columns: bool = True,
        name_prefix: str | None = None,
    ) -> pl.DataFrame:
        return _helpers.decode_events(
            events=self._df,
            event_abi=event_abi,
            columns=columns,
            drop_raw_columns=drop_raw_columns,
            name_prefix=name_prefix,
        )
