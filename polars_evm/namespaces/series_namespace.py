from __future__ import annotations

import typing

import polars as pl

from .. import _helpers


@pl.api.register_series_namespace('evm')
class SeriesEvm:
    def __init__(self, series: pl.Series):
        self._series = series

    def binary_to_hex(self, prefix: bool = True) -> pl.Series:
        return _helpers.binary_series_to_hex(self._series, prefix=prefix)

    def hex_to_binary(self, prefix: bool | None = None) -> pl.Series:
        return _helpers.hex_series_to_binary(self._series, prefix=prefix)

    def binary_to_float(self, raw_type: str) -> pl.Seties:
        return _helpers.binary_series_to_float(
            hex_series=self._series, raw_type=raw_type
        )

    def keccak(
        self,
        output: typing.Literal[
            'hex', 'binary', 'prefix_hex', 'raw_hex'
        ] = 'hex',
        text: bool = False,
    ) -> pl.Series:
        return self._series.map_elements(
            lambda datum: _helpers.keccak(datum, output=output, text=text)
        )
