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
        if columns is None:
            columns = []
            for column, dtype in zip(self._lf.columns, self._lf.dtypes):
                if dtype == pl.datatypes.Binary:
                    columns.append(column)

        if prefix:
            return self._lf.with_columns(
                [
                    pl.lit('0x') + pl.col(column).bin.encode('hex')
                    for column in columns
                ]
            )
        else:
            return self._lf.with_columns(
                [pl.col(column).bin.encode('hex') for column in columns]
            )

    def hex_to_binary(
        self, columns: typing.Sequence[str] | None = None, prefix: bool = True
    ) -> pl.LazyFrame:
        if columns is None:
            columns = []
            for column, dtype in zip(self._lf.columns, self._lf.dtypes):
                if dtype == pl.datatypes.Binary:
                    columns.append(column)

        if prefix:
            return self._lf.with_columns(
                [
                    pl.col(column).slice(2).str.decode('hex')
                    for column in columns
                ]
            )
        else:
            return self._lf.with_columns(
                [pl.col(column).str.decode('hex') for column in columns]
            )

    def binary_to_float(
        self,
        column_types: dict[str, str],
        replace: bool = False,
    ) -> pl.LazyFrame:
        return _helpers.binary_df_to_float(
            df=self._lf, column_types=column_types, replace=replace
        )
