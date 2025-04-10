from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

from .. import conversions
from . import decoding_types


def decode_df(
    df: pl.DataFrame,
    column_types: dict[str, str | decoding_types.AbiType],
    *,
    padded: bool = True,
    prefix: bool = True,
    hex_output: bool = False,
    replace: bool = False,
) -> pl.DataFrame:
    import polars as pl

    hex_exprs = {}
    decode_exprs = {}
    schema = df.collect_schema()
    for name, abi_type in column_types.items():
        column_dtype = schema.get(name)
        if column_dtype == pl.String:
            hex_expr = pl.col(name)
        elif column_dtype == pl.Binary:
            hex_name = name + 'as_hex'
            hex_exprs[hex_name] = pl.col.name.bin.encode('hex')
            hex_expr = pl.col(hex_name)
        else:
            raise Exception('invalid column type')

        if not replace:
            name = name + '_decoded'

        decode_exprs[name] = decode_hex_expr(
            hex_expr,
            abi_type=abi_type,
            padded=padded,
            prefix=prefix,
            hex_output=hex_output,
        )

    return (
        df.with_columns(**hex_exprs)
        .with_columns(**decode_exprs)
        .drop(*hex_exprs.keys())
    )


def decode_hex_series(
    series: pl.Series,
    abi_type: str | decoding_types.AbiType,
    *,
    padded: bool = True,
    prefix: bool = True,
    hex_output: bool = False,
) -> pl.Series:
    expr = decode_hex_expr(
        pl.col.as_hex,
        abi_type=abi_type,
        padded=padded,
        prefix=prefix,
        hex_output=hex_output,
    )
    return pl.DataFrame({'as_hex': series}).select(decoded=expr)['decoded']


def decode_hex_expr(
    expr: pl.Expr,
    abi_type: str | decoding_types.AbiType,
    *,
    padded: bool = True,
    prefix: bool = True,
    hex_output: bool = False,
    max_array_length: int = 32,
) -> pl.Expr:
    """
    dynamic types not yet implemented

    - padded: True if there might be leftside padding
    - prefix: True if there might be a '0x' prefix on entries

    see abi spec here https://docs.soliditylang.org/en/develop/abi-spec.html
    """
    import polars as pl

    if isinstance(abi_type, str):
        abi_type = decoding_types.parse_abi_type(abi_type)
    type_name = abi_type['name']

    # preprocess expr
    if (
        padded
        and abi_type['n_bits'] is not None
        and abi_type['n_bits'] < 256
        and not abi_type['has_tail']
        and abi_type['name'] != 'bytes'
        and not abi_type['name'].endswith(']')
    ):
        if type_name.startswith('bytes'):
            if prefix:
                expr = expr.str.strip_prefix('0x')
            expr = expr.str.slice(0, int(abi_type['n_bits'] / 4))
        else:
            expr = expr.str.slice(int(-abi_type['n_bits'] / 4))
    elif prefix:
        expr = expr.str.strip_prefix('0x')

    # decode type
    if type_name.endswith(']'):
        return _decode_array(expr, abi_type, hex_output, max_array_length)
    elif type_name.endswith(')'):
        return _decode_tuple(expr, abi_type, hex_output)
    elif type_name == 'bytes':
        if padded:
            length = _hex_to_int(expr.str.slice(48, 16), pl.UInt64)
            return _format_binary(expr.str.slice(64, length * 2), hex_output)
        else:
            return _format_binary(expr, hex_output)
    elif type_name == 'string':
        return expr.str.decode('hex').cast(pl.String)
    elif type_name == 'address':
        return _format_binary(expr, hex_output)
    elif type_name == 'bool':
        return expr.str.slice(-1) != '0'
    elif type_name.startswith('int'):
        return _decode_hex_signed_int(expr, abi_type)
    elif type_name.startswith('uint'):
        return _decode_hex_unsigned_int(expr, abi_type)
    elif type_name.startswith('bytes'):
        return _format_binary(expr, hex_output)
    elif type_name.startswith('fixed'):
        f64 = decode_hex_expr(
            expr, 'int' + str(abi_type['n_bits']), padded=False
        )
        if abi_type['fixed_scale'] is None:
            raise Exception('must specify fixed_scale')
        return f64 / (10.0 ** pl.lit(int(abi_type['fixed_scale'])))
    elif type_name.startswith('ufixed'):
        f64 = decode_hex_expr(
            expr, 'uint' + str(abi_type['n_bits']), padded=False
        )
        if abi_type['fixed_scale'] is None:
            raise Exception('must specify fixed_scale')
        return f64 / (10.0 ** pl.lit(int(abi_type['fixed_scale'])))
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
    hex_output: bool,
    max_array_length: int,
) -> pl.Expr:
    import polars as pl

    subtype = abi_type['array_type']
    if subtype is None:
        raise Exception('must specify array type')

    exprs = []
    if abi_type['array_length'] is not None:
        array_length = abi_type['array_length']
        pre_offset = 0
    else:
        array_length = max_array_length
        pre_offset = 64
    for i in range(array_length):
        if subtype['has_tail']:
            offset = _hex_to_int(
                expr.str.slice(pre_offset + i * 64 + 48, 16), pl.UInt64
            )
            if subtype['static']:
                tail_length_bytes: int | pl.Expr = subtype['n_bits'] // 8  # type: ignore
            else:
                tail_length_bytes = _hex_to_int(
                    expr.str.slice(pre_offset + offset + 48, 16), pl.UInt64
                )
                offset += 32
            body = expr.str.slice(
                pre_offset + offset * 2, tail_length_bytes * 2
            )
        else:
            body = expr.str.slice(pre_offset + i * 64, 64)
        subexpr = decode_hex_expr(body, subtype, hex_output=hex_output)
        exprs.append(subexpr)

    output = pl.concat_list(exprs)

    if abi_type['array_length'] is None:
        if subtype['name'].startswith('bytes'):
            output = output.list.eval(pl.element().filter(pl.element() != '0x'))
        elif subtype['name'].endswith(')'):
            field_names = _get_tuple_field_names(subtype)
            output = output.list.eval(
                pl.element().filter(
                    pl.element().struct.field(*field_names).null_count()
                    < len(field_names)
                )
            )
        else:
            output = output.list.eval(
                pl.element().filter(pl.element().is_not_null())
            )

    return output


def _get_tuple_field_names(abi_type: decoding_types.AbiType) -> list[str]:
    tuple_types = abi_type['tuple_types']
    if tuple_types is None:
        raise Exception('not a tuple type')

    tuple_names = abi_type['tuple_names']
    if tuple_names is None:
        tuple_names = [None] * len(tuple_types)
    output_names = []
    for i, name in enumerate(tuple_names):
        if name is None:
            name = 'field' + str(i)
        output_names.append(name)
    return output_names


def _decode_tuple(
    expr: pl.Expr,
    abi_type: decoding_types.AbiType,
    hex_output: bool = False,
) -> pl.Expr:
    import polars as pl

    if abi_type['name'] == '()':
        return pl.lit({})

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
            field = decode_hex_expr(
                expr=expr.str.slice(i * 64, 64),
                abi_type=subtype,
                padded=True,
                prefix=False,
                hex_output=hex_output,
            )
        else:
            offset = _hex_to_int(expr.str.slice(i * 64 + 48, 16), pl.UInt64)
            tail_length = _hex_to_int(
                expr.str.slice(offset * 2 + 48, 16), pl.UInt64
            )

            # TODO: if tuple contains subtypes with tails, add to tail_length
            # the offset and subtail length of the last tailed subtype in tuple
            if (
                subtype['array_type'] is not None
                and (subtype['array_type']['has_tail'])
            ):
                raise NotImplementedError(
                    'decoding tuples of arrays: ' + str(abi_type['name'])
                )
            if subtype['tuple_types'] is not None and any(
                tuple_type['has_tail'] for tuple_type in subtype['tuple_types']
            ):
                raise NotImplementedError(
                    'decoding nested dynammic tuples: ' + str(abi_type['name'])
                )

            padded = True
            if subtype['name'] in ('string', 'bytes'):
                padded = False
                offset = offset + 32
            else:
                tail_length = (tail_length + 1) * 32
            data = expr.str.slice(offset * 2, tail_length * 2)
            field = decode_hex_expr(
                data,
                subtype,
                padded=padded,
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
