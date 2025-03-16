from __future__ import annotations

import typing

from . import decoding_columns

if typing.TYPE_CHECKING:
    import polars as pl


def decode_transactions(
    transactions: pl.DataFrame,
    *,
    function_abi: dict[str, typing.Any] | None = None,
    contract_abi: list[dict[str, typing.Any]] | None = None,
    ignore_unknown: bool = False,
) -> pl.DataFrame | dict[str, pl.DataFrame]:
    if function_abi is None and contract_abi is None:
        raise Exception('specify function_abi or contract_abi')
    elif function_abi is not None and contract_abi is not None:
        raise Exception('do not specify both function_abi and contract_abi')
    elif function_abi is not None and contract_abi is None:
        return _decode_transactions_function_abi(transactions, function_abi)
    elif function_abi is None and contract_abi is not None:
        return _decode_transactions_contract_abi(
            transactions, contract_abi, ignore_unknown=ignore_unknown
        )
    else:
        raise Exception()


def _decode_transactions_contract_abi(
    transactions: pl.DataFrame,
    contract_abi: list[dict[str, typing.Any]],
    ignore_unknown: bool,
) -> dict[str, pl.DataFrame]:
    import ctc

    transactions = transactions.with_columns(
        input_hex=pl.col.input.bin.encode('hex'),
    ).with_columns(
        selector=pl.col.input_hex.str.slice(0, 8),
    )

    abis_by_selector = {
        ctc.get_function_selector(function_abi): function_abi  # type: ignore
        for function_abi in contract_abi
        if function_abi['type'] == 'function'
    }

    if ignore_unknown:
        transactions = transactions.filter(
            pl.col.selector.is_in(abis_by_selector.keys())
        )

    output = {}
    partitions = transactions.partition_by('selector', as_dict=True)
    for selector_tuple, sub_txs in partitions.items():
        selector: str = selector_tuple[0]  # type: ignore
        output[selector] = _decode_transactions_function_abi(
            transactions=sub_txs,
            function_abi=abis_by_selector[selector],
        )

    return output


def _decode_transactions_function_abi(
    transactions: pl.DataFrame,
    function_abi: dict[str, typing.Any],
) -> pl.DataFrame:
    import ctc

    function_selector = ctc.get_function_selector(function_abi)  # type: ignore

    cols = {}
    for i, input in enumerate(function_abi['inputs']):
        cols[input['name']] = decoding_columns.decode_hex_expr(
            pl.col.input_hex.str.slice(64 * i, 64),
            input['type'],
            padded=True,
            prefix=False,
        )

    return (
        transactions.with_columns(input_hex=pl.col.input.bin.encode('hex'))
        .filter(pl.col.input_hex.str.starts_with(function_selector))
        .with_columns(
            selector=pl.col.input_hex.str.slice(0, 8),
            function_data=pl.col.input_hex.str.slice(8),
            function_name=pl.lit(function_abi['name']),
            **cols,
        )
        .drop('input_hex')
    )
