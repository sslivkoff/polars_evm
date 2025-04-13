from __future__ import annotations

import typing

import polars as pl

from .. import _helpers


@pl.api.register_lazyframe_namespace('evm')
class LazyFrameEvm:
    def __init__(self, lf: pl.LazyFrame):
        self._lf = lf

    def decode_events(
        self,
        event_abi: dict[str, typing.Any],
        *,
        columns: list[str] | None = None,
        drop_raw_columns: bool = True,
        name_prefix: str | None = None,
        hex_output: bool = False,
    ) -> pl.LazyFrame:
        return _helpers.decode_events(
            events=self._lf,
            event_abi=event_abi,
            columns=columns,
            drop_raw_columns=drop_raw_columns,
            name_prefix=name_prefix,
            hex_output=hex_output,
        )

    def filter_binary(self, **column_addresses: typing.Any) -> pl.LazyFrame:
        return _helpers.filter_binary(self._lf, column_addresses)

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
