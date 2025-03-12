from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

from .. import conversions
from . import decoding_types


def decode_hex(
    expr: pl.Expr,
    abi_type: str,
    *,
    padded: bool = True,
    prefix: bool = True,
    hex_output: bool = True,
) -> pl.Expr:
    """
    dynamic types not yet implemented

    - padded: True if there might be leftside padding
    - prefix: True if there might be a '0x' prefix on entries

    see abi spec here https://docs.soliditylang.org/en/develop/abi-spec.html
    """
    # preprocess expr
    if padded:
        n_bits = decoding_types.get_static_n_bits(abi_type)
        if n_bits < 256:
            expr = expr.str.slice(int(-n_bits / 4))
    elif prefix:
        expr = expr.str.strip_prefix('0x')

    # make type fully explicit
    if abi_type == 'int':
        abi_type = 'int256'
    elif abi_type == 'uint':
        abi_type = 'uint256'
    elif abi_type == 'fixed':
        abi_type = 'fixed128x18'
    elif abi_type == 'ufixed':
        abi_type = 'ufixed128x18'

    if abi_type.endswith(']'):
        raise NotImplementedError('array decoding not implemented')
    elif abi_type.endswith(')'):
        raise NotImplementedError('tuple decoding not implemented')
    elif abi_type == 'bytes':
        raise NotImplementedError('bytes')
    elif abi_type == 'address':
        if hex_output:
            return '0x' + expr
        else:
            return expr.str.decode('hex')
    elif abi_type == 'boolean':
        return expr.str.slice(-1) != '0'
    elif abi_type.startswith('int'):
        return _decode_hex_signed_int(expr, abi_type)
    elif abi_type.startswith('uint'):
        return _decode_hex_unsigned_int(expr, abi_type)
    elif abi_type.startswith('bytes'):
        if hex_output:
            return '0x' + expr
        else:
            return expr.str.decode('hex')
    elif abi_type.startswith('fixed'):
        _, _, scale = abi_type[5:].split('x')
        as_float = decode_hex(expr, 'int' + str(n_bits), padded=padded)
        return as_float / (10 ** pl.lit(int(scale)))
    elif abi_type.startswith('ufixed'):
        _, _, scale = abi_type[6:].split('x')
        as_float = decode_hex(expr, 'uint' + str(n_bits), padded=padded)
        return as_float / (10 ** pl.lit(int(scale)))
    elif abi_type == 'function':
        if hex_output:
            return pl.struct(
                address='0x' + expr.str.slice(-48, 40),
                selector='0x' + expr.str.slice(-8, 8),
            )
        else:
            return pl.struct(
                address=expr.str.slice(-48, 40).str.decode('hex'),
                selector=expr.str.slice(-8, 8).str.decode('hex'),
            )
    else:
        raise Exception()


def _decode_hex_signed_int(expr: pl.Expr, abi_type: str) -> pl.Expr:
    import polars as pl

    n_bits = int(abi_type[3:])
    if n_bits % 8 != 0:
        raise Exception('n_bits must be multiple of 8')
    elif n_bits <= 0:
        raise Exception('n_bits must be positive')
    if n_bits == 8:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.Int8, endianness='big'
        )
    elif n_bits == 16:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.Int16, endianness='big'
        )
    elif n_bits == 32:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.Int32, endianness='big'
        )
    elif n_bits == 64:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.Int64, endianness='big'
        )
    elif n_bits == 24:
        is_negative = expr.str.slice(0, 2).str.to_lowercase() > '7f'
        return (
            pl.when(is_negative)
            .then('FF' + expr)
            .otherwise('00' + expr)
            .str.decode('hex')
            .bin.reinterpret(dtype=pl.Int32, endianness='big')
        )
    elif n_bits < 64:
        is_negative = expr.str.slice(0, 2).str.to_lowercase() > '7f'
        n_padding_bytes = int((64 - n_bits) / 2)
        return (
            pl.when(is_negative)
            .then('FF' * n_padding_bytes + expr)
            .otherwise('00' * n_padding_bytes + expr)
            .str.decode('hex')
            .bin.reinterpret(dtype=pl.Int64, endianness='big')
        )
    elif n_bits > 64:
        return conversions.hex_expr_to_float(expr, 'i' + str(n_bits))
    else:
        raise Exception('invalid number of bits')


def _decode_hex_unsigned_int(expr: pl.Expr, abi_type: str) -> pl.Expr:
    import polars as pl

    n_bits = int(abi_type[4:])
    if n_bits % 8 != 0:
        raise Exception('n_bits must be multiple of 8')
    elif n_bits <= 0:
        raise Exception('n_bits must be positive')
    if n_bits == 8:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.UInt8, endianness='big'
        )
    elif n_bits == 16:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.UInt16, endianness='big'
        )
    elif n_bits == 32:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.UInt32, endianness='big'
        )
    elif n_bits == 64:
        return expr.str.decode('hex').bin.reinterpret(
            dtype=pl.UInt64, endianness='big'
        )
    elif n_bits == 24:
        return (
            ('00' + expr)
            .str.decode('hex')
            .bin.reinterpret(dtype=pl.UInt32, endianness='big')
        )
    elif n_bits < 64:
        n_padding_bytes = int((64 - n_bits) / 2)
        return (
            ('00' * n_padding_bytes + expr)
            .str.decode('hex')
            .bin.reinterpret(dtype=pl.UInt64, endianness='big')
        )
    elif n_bits > 64:
        return conversions.hex_expr_to_float(expr, 'u' + str(n_bits))
    else:
        raise Exception('invalid number of bits')
