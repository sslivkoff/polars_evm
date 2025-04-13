from __future__ import annotations

import typing

from . import decoding_columns

if typing.TYPE_CHECKING:
    import polars as pl

    _T = typing.TypeVar('_T', pl.DataFrame, pl.LazyFrame)


def decode_events(
    events: _T,
    event_abi: dict[str, typing.Any],
    *,
    columns: list[str] | None = None,
    drop_raw_columns: bool = True,
    name_prefix: str | None = None,
    hex_output: bool = False,
) -> _T:
    import polars as pl

    # decide which columns to decode
    if columns is None:
        columns = [input['name'] for input in event_abi.get('inputs', [])]

    # gather abi info
    input_abis = {i['name']: i for i in event_abi['inputs']}
    indexed = [i['name'] for i in event_abi['inputs'] if i['indexed']]
    unindexed = [i['name'] for i in event_abi['inputs'] if not i['indexed']]

    # build columns
    schema = events.collect_schema()
    temp_exprs = {}
    column_exprs = {}
    for column in columns:
        # get raw column expr
        if column in indexed:
            raw_column = 'topic' + str(indexed.index(column) + 1)
        else:
            raw_column = 'data'

        # create temp hex column as needed
        schema_dtype = schema.get(raw_column)
        if schema_dtype == pl.Binary:
            hex_name = raw_column + '_hex'
            temp_exprs[hex_name] = pl.col(raw_column).bin.encode('hex')
            expr = pl.col(hex_name)
            padded = True
            prefix = False
        elif schema_dtype == pl.String:
            expr = pl.col(raw_column)
            padded = True
            prefix = True
        else:
            raise Exception()
        if column not in indexed:
            expr = expr.str.slice(64 * unindexed.index(column), 64)

        # decode column expression
        column_exprs[column] = decoding_columns.decode_hex_expr(
            expr=expr,
            abi_type=input_abis[column]['type'],
            padded=padded,
            prefix=prefix,
            hex_output=hex_output,
        )

    # insert prefix
    if name_prefix is None and any(k in events.columns for k in column_exprs):
        name_prefix = 'event__'
    if name_prefix is not None:
        column_exprs = {name_prefix + k: v for k, v in column_exprs.items()}

    # decide which columns to drop
    drop = []
    if drop_raw_columns:
        drop = ['topic0', 'topic1', 'topic2', 'topic3', 'data']

    return (
        events.filter(_get_event_filters(schema, event_abi))
        .with_columns(**temp_exprs)
        .with_columns(**column_exprs)
        .drop(*temp_exprs.keys(), *drop)
    )


def decode_contract_events(
    events: pl.DataFrame,
    contract_abi: list[dict[str, typing.Any]],
    *,
    drop_raw_columns: bool = True,
    name_prefix: str | None = None,
    hex_output: bool = False,
    ignore_unknown: bool = False,
    key: typing.Literal['topic0', 'name'] | None = None,
) -> dict[str, pl.DataFrame]:
    import ctc
    import polars as pl

    events = events.with_columns(
        data_hex=pl.col.data.bin.encode('hex'),
    ).with_columns(selector=pl.col.topic0)

    event_abis: list[dict[str, typing.Any]] = [
        event_abi for event_abi in contract_abi if event_abi['type'] == 'event'
    ]
    names = [event_abi['name'] for event_abi in event_abis]
    topic0s = [
        bytes.fromhex(ctc.get_event_hash(event_abi)[2:])  # type: ignore
        for event_abi in event_abis
    ]
    abis_by_selector = dict(zip(topic0s, event_abis))

    if ignore_unknown:
        events = events.filter(pl.col.selector.is_in(topic0s))

    if key is None:
        if len(names) == len(set(names)):
            key = 'name'
        else:
            key = 'topic0'
    if key == 'name':
        keys = names
    elif key == 'topic0':
        keys = topic0s
    else:
        raise Exception()

    output = {}
    partitions = events.partition_by('selector', as_dict=True)
    for df_key, (selector_tuple, sub_events) in zip(keys, partitions.items()):
        selector: bytes = selector_tuple[0]  # type: ignore
        output[df_key] = decode_events(
            events=sub_events,
            event_abi=abis_by_selector[selector],
            drop_raw_columns=drop_raw_columns,
            name_prefix=name_prefix,
            hex_output=hex_output,
        )

    return output


def get_event_hash(event_abi: dict[str, typing.Any]) -> str:
    import ctc

    return ctc.get_event_hash(event_abi)  # type: ignore


def _get_event_filters(
    schema: pl.Schema, event_abi: dict[str, typing.Any]
) -> list[pl.Expr]:
    import polars as pl

    n_indexed_columns = 0
    has_data_column = False
    for input in event_abi['inputs']:
        if input['indexed']:
            n_indexed_columns += 1
        else:
            has_data_column = True

    filters = []

    # topic0 filter
    event_hash = get_event_hash(event_abi)[2:]
    if schema.get('topic0') == pl.String:
        filters.append(
            pl.col.topic0.str.strip_prefix('0x') == pl.lit(event_hash)
        )
    else:
        filters.append(pl.col.topic0 == bytes.fromhex(event_hash))

    # null checks
    if n_indexed_columns == 0:
        filters.append(pl.col.topic1.is_null())
        filters.append(pl.col.topic2.is_null())
        filters.append(pl.col.topic3.is_null())
    elif n_indexed_columns == 1:
        filters.append(pl.col.topic1.is_not_null())
        filters.append(pl.col.topic2.is_null())
        filters.append(pl.col.topic3.is_null())
    elif n_indexed_columns == 2:
        filters.append(pl.col.topic1.is_not_null())
        filters.append(pl.col.topic2.is_not_null())
        filters.append(pl.col.topic3.is_null())
    elif n_indexed_columns == 3:
        filters.append(pl.col.topic1.is_not_null())
        filters.append(pl.col.topic2.is_not_null())
        filters.append(pl.col.topic3.is_not_null())
    else:
        raise Exception('invalid number of indexed columns')
    if has_data_column:
        filters.append(pl.col.data != b'')
    else:
        filters.append(pl.col.data == b'')

    return filters
