from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

from .. import conversions
from . import decoding_types


def decode_hex(
    expr: pl.Expr,
    abi_type: str | decoding_types.AbiType,
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
    if isinstance(abi_type, str):
        abi_type = decoding_types.parse_abi_type(abi_type)
    type_name = abi_type['name']

    # preprocess expr
    if padded:
        if abi_type['n_bits'] is not None and abi_type['n_bits'] < 256:
            expr = expr.str.slice(int(-abi_type['n_bits'] / 4))
    elif prefix:
        expr = expr.str.strip_prefix('0x')

    if padded and type_name in ['bytes', 'string']:
        raise Exception(type_name + ' cannot be padded, use padded=False')

    # decode type
    if type_name.endswith(']'):
        return _decode_array(expr, abi_type, hex_output)
    elif type_name.endswith(')'):
        return _decode_tuple(expr, abi_type, hex_output)
    elif type_name == 'bytes':
        return _format_binary(expr, hex_output)
    elif type_name == 'string':
        return expr.str.decode('hex').cast(pl.String)
    elif type_name == 'address':
        return _format_binary(expr, hex_output)
    elif type_name == 'boolean':
        return expr.str.slice(-1) != '0'
    elif type_name.startswith('int'):
        return _decode_hex_signed_int(expr, abi_type)
    elif type_name.startswith('uint'):
        return _decode_hex_unsigned_int(expr, abi_type)
    elif type_name.startswith('bytes'):
        return _format_binary(expr, hex_output)
    elif type_name.startswith('fixed'):
        f64 = decode_hex(expr, 'int' + str(abi_type['n_bits']), padded=padded)
        if abi_type['fixed_scale'] is None:
            raise Exception('must specify fixed_scale')
        return f64 / (10 ** pl.lit(int(abi_type['fixed_scale'])))
    elif type_name.startswith('ufixed'):
        f64 = decode_hex(expr, 'uint' + str(abi_type['n_bits']), padded=padded)
        if abi_type['fixed_scale'] is None:
            raise Exception('must specify fixed_scale')
        return f64 / (10 ** pl.lit(int(abi_type['fixed_scale'])))
    elif type_name == 'function':
        return pl.struct(
            address=_format_binary(expr.str.slice(-48, 40), hex_output),
            selector=_format_binary(expr.str.slice(-8, 8), hex_output),
        )
    else:
        raise Exception()


def _format_binary(expr: pl.Expr, hex_output: bool) -> pl.Expr:
    if hex_output:
        return '0x' + expr
    else:
        return expr.str.decode('hex')


def _decode_array(
    expr: pl.Expr,
    abi_type: decoding_types.AbiType,
    hex_output: bool = True,
) -> pl.Expr:
    subtype = abi_type['array_type']
    if subtype is None:
        raise Exception('must specify array type')
    if abi_type['array_length'] is not None:
        if subtype['has_tail']:
            exprs = []
            for i in range(abi_type['array_length']):
                offset = _hex_to_int(expr.str.slice(i * 64, 64), pl.UInt64)
                if subtype['static']:
                    if abi_type['n_bits'] is None:
                        raise Exception('must specify static n_bits')
                    tail_length: int | pl.Expr = abi_type['n_bits'] // 4
                else:
                    tail_length = expr.str.slice(offset)
                    offset += 1
                tail_body = expr.str.slice(offset * 2, tail_length * 2)
                subexpr = decode_hex(tail_body, subtype, hex_output=hex_output)
                exprs.append(subexpr)
        else:
            exprs = [
                decode_hex(
                    expr.slice(i * 64, 64), subtype, hex_output=hex_output
                )
                for i in range(abi_type['array_length'])
            ]
    else:
        raise NotImplementedError('dynamic arrays')
    return pl.concat_list(exprs)


def _decode_tuple(
    expr: pl.Expr,
    abi_type: decoding_types.AbiType,
    hex_output: bool = True,
) -> pl.Expr:
    tuple_names = abi_type['tuple_names']
    tuple_types = abi_type['tuple_types']
    if tuple_types is None:
        raise Exception('tuple_types must be specified')
    if tuple_names is None:
        tuple_names = [None] * len(tuple_types)
    fields = []
    for i, (name, subtype) in enumerate(zip(tuple_names, tuple_types)):
        if name is None:
            name = 'field' + str(i)
        if not subtype['has_tail']:
            field = decode_hex(
                expr=expr.str.slice(i * 64, 64),
                abi_type=subtype,
                padded=True,
                prefix=False,
                hex_output=hex_output,
            )
        else:
            offset = _hex_to_int(expr.str.slice(i * 64, 64), pl.UInt64)
            tail_length = _hex_to_int(expr.str.slice(offset * 2, 64), pl.UInt64)
            field = decode_hex(
                expr=expr.str.slice((offset + 1) * 2, tail_length * 2),
                abi_type=subtype,
                padded=True,
                prefix=False,
                hex_output=hex_output,
            )
        fields.append(field.alias(name))
    return pl.struct(fields)


def _hex_to_int(expr: pl.Expr, dtype: type[pl.DataType]) -> pl.Expr:
    return expr.str.decode('hex').bin.reinterpret(dtype=dtype, endianness='big')


def _decode_hex_signed_int(
    expr: pl.Expr, abi_type: decoding_types.AbiType
) -> pl.Expr:
    import polars as pl

    n_bits = abi_type['n_bits']
    if n_bits is None:
        raise Exception('n_bits must be specified')
    if n_bits % 8 != 0:
        raise Exception('n_bits must be multiple of 8')
    elif n_bits <= 0:
        raise Exception('n_bits must be positive')
    if n_bits == 8:
        return _hex_to_int(expr, pl.Int8)
    elif n_bits == 16:
        return _hex_to_int(expr, pl.Int16)
    elif n_bits == 32:
        return _hex_to_int(expr, pl.Int32)
    elif n_bits == 64:
        return _hex_to_int(expr, pl.Int64)
    elif n_bits == 24:
        is_negative = expr.str.slice(0, 2).str.to_lowercase() > '7f'
        full = pl.when(is_negative).then('FF' + expr).otherwise('00' + expr)
        return _hex_to_int(full, pl.Int32)
    elif n_bits < 64:
        is_negative = expr.str.slice(0, 2).str.to_lowercase() > '7f'
        n_padding_bytes = int((64 - n_bits) / 2)
        full = (
            pl.when(is_negative)
            .then('FF' * n_padding_bytes + expr)
            .otherwise('00' * n_padding_bytes + expr)
        )
        return _hex_to_int(full, pl.Int64)
    elif n_bits > 64:
        return conversions.hex_expr_to_float(expr, 'i' + str(n_bits))
    else:
        raise Exception('invalid number of bits')


def _decode_hex_unsigned_int(
    expr: pl.Expr, abi_type: decoding_types.AbiType
) -> pl.Expr:
    import polars as pl

    n_bits = abi_type['n_bits']
    if n_bits is None:
        raise Exception('n_bits must be specified')
    if n_bits % 8 != 0:
        raise Exception('n_bits must be multiple of 8')
    elif n_bits <= 0:
        raise Exception('n_bits must be positive')
    if n_bits == 8:
        return _hex_to_int(expr, pl.UInt8)
    elif n_bits == 16:
        return _hex_to_int(expr, pl.UInt16)
    elif n_bits == 32:
        return _hex_to_int(expr, pl.UInt32)
    elif n_bits == 64:
        return _hex_to_int(expr, pl.UInt64)
    elif n_bits == 24:
        return _hex_to_int('00' + expr, pl.UInt32)
    elif n_bits < 64:
        n_padding_bytes = int((64 - n_bits) / 2)
        return _hex_to_int('00' * n_padding_bytes + expr, pl.UInt64)
    elif n_bits > 64:
        return conversions.hex_expr_to_float(expr, 'u' + str(n_bits))
    else:
        raise Exception('invalid number of bits')
