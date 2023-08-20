from __future__ import annotations

import typing

import polars as pl

from .. import _helpers


@pl.api.register_series_namespace('evm')
class SeriesEvm:
    def __init__(self, series: pl.Series):
        self._series = series

    def binary_to_hex(self, prefix: bool = True) -> pl.Series:
        if prefix:
            return _helpers.binary_series_to_prefix_hex(self._series)
        else:
            return _helpers.binary_series_to_raw_hex(self._series)

    def hex_to_binary(self, prefix: bool = True) -> pl.Series:
        if prefix:
            return _helpers.prefix_hex_series_to_binary(self._series)
        else:
            return _helpers.raw_hex_series_to_binary(self._series)

    def keccak(
        self,
        output: typing.Literal[
            'hex', 'binary', 'prefix_hex', 'raw_hex'
        ] = 'hex',
        text: bool = False,
    ) -> pl.Series:
        return self._series.apply(
            lambda datum: _helpers.keccak(datum, output=output, text=text)
        )

