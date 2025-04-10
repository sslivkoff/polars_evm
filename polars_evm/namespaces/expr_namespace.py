from __future__ import annotations

import typing

import polars as pl

from .. import _helpers


@pl.api.register_expr_namespace('evm')
class ExprEvm:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def binary_to_hex(self, prefix: bool = True) -> pl.Expr:
        return _helpers.binary_expr_to_hex(self._expr, prefix=prefix)

    def hex_to_binary(self, prefix: bool = True) -> pl.Expr:
        return _helpers.hex_expr_to_binary(self._expr, prefix=prefix)

    def binary_to_float(self, raw_type: str) -> pl.Expr:
        return _helpers.hex_expr_to_float(
            self._expr.bin.encode('hex'), raw_type=raw_type
        )

    def hex_to_float(self, raw_type: str) -> pl.Expr:
        return _helpers.hex_expr_to_float(self._expr, raw_type=raw_type)

    def decode_hex(
        self,
        abi_type: str,
        *,
        padded: bool = True,
        prefix: bool = True,
        hex_output: bool = False,
    ) -> pl.Expr:
        return _helpers.decode_hex_expr(
            self._expr,
            abi_type=abi_type,
            padded=padded,
            prefix=prefix,
            hex_output=hex_output,
        )

    def keccak(
        self,
        output: typing.Literal[
            'hex', 'binary', 'prefix_hex', 'raw_hex'
        ] = 'hex',
        text: bool = False,
    ) -> pl.Expr:
        return self._expr.map_elements(
            lambda datum: _helpers.keccak(datum, output=output, text=text)
        )
