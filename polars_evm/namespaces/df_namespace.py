from __future__ import annotations

import typing

import polars as pl

from .. import _helpers


@pl.api.register_dataframe_namespace('evm')
class DataFrameEvm:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def filter_binary(self, **column_addresses: typing.Any) -> pl.DataFrame:
        return _helpers.filter_binary(self._df, column_addresses)

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

    def decode(
        self,
        column_types: dict[str, str | _helpers.AbiType],
        *,
        padded: bool = True,
        prefix: bool = True,
        hex_output: bool = False,
        replace: bool = False,
    ) -> pl.DataFrame:
        return _helpers.decode_df(
            df=self._df,
            column_types=column_types,
            padded=padded,
            prefix=prefix,
            hex_output=hex_output,
            replace=replace,
        )

    def decode_events(
        self,
        event_abi: dict[str, typing.Any],
        *,
        columns: list[str] | None = None,
        drop_raw_columns: bool = True,
        name_prefix: str | None = None,
        hex_output: bool = False,
    ) -> pl.DataFrame:
        return _helpers.decode_events(
            events=self._df,
            event_abi=event_abi,
            columns=columns,
            drop_raw_columns=drop_raw_columns,
            name_prefix=name_prefix,
            hex_output=hex_output,
        )

    def decode_contract_events(
        self,
        contract_abi: list[dict[str, typing.Any]],
        *,
        drop_raw_columns: bool = True,
        name_prefix: str | None = None,
        hex_output: bool = False,
        ignore_unknown: bool = False,
        key: typing.Literal['topic0', 'name'] | None = None,
    ) -> dict[str, pl.DataFrame]:
        return _helpers.decode_contract_events(
            events=self._df,
            contract_abi=contract_abi,
            drop_raw_columns=drop_raw_columns,
            name_prefix=name_prefix,
            hex_output=hex_output,
            ignore_unknown=ignore_unknown,
            key=key,
        )

    def decode_transactions(
        self,
        *,
        function_abi: dict[str, typing.Any] | None = None,
        contract_abi: list[dict[str, typing.Any]] | None = None,
        ignore_unknown: bool = False,
    ) -> pl.DataFrame | dict[str, pl.DataFrame]:
        return _helpers.decode_transactions(
            transactions=self._df,
            function_abi=function_abi,
            contract_abi=contract_abi,
            ignore_unknown=ignore_unknown,
        )
