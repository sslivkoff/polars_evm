from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import polars as pl

    _T = typing.TypeVar('_T', pl.DataFrame, pl.LazyFrame)


def filter_binary(df: _T, column_values: dict[str, list[str]]) -> _T:
    import polars as pl

    filter: bool | pl.Expr
    schema = df.collect_schema()
    filters = []
    for column_name, values in column_values.items():
        column_dtype = schema.get(column_name)

        if column_dtype == pl.Binary:
            if isinstance(values, pl.Series):
                if values.dtype == pl.Series:
                    filter = pl.col(column_name).is_in(values)
                elif values.dtype == pl.String:
                    filter = pl.col(column_name).is_in(
                        values.evm.hex_to_binary()
                    )
                else:
                    raise Exception()
            elif isinstance(values, str):
                filter = pl.col(column_name) == bytes.fromhex(values[2:])
            elif isinstance(values, bytes):
                filter = pl.col(column_name) == values
            elif isinstance(values, list):
                if len(values) == 0:
                    filter = False
                elif isinstance(values[0], bytes):
                    filter = pl.col(column_name).is_in(values)
                elif isinstance(values[0], str):
                    filter = pl.col(column_name).is_in(
                        [bytes.fromhex(address[2:]) for address in values]
                    )
                else:
                    raise Exception()
            else:
                raise Exception()

        elif column_dtype == pl.String:
            if isinstance(values, pl.Series):
                if values.dtype == pl.Series:
                    filter = pl.col(column_name).is_in(
                        values.evm.binary_to_hex()
                    )
                elif values.dtype == pl.String:
                    filter = pl.col(column_name).is_in(values)
                else:
                    raise Exception()
            elif isinstance(values, str):
                filter = pl.col(column_name) == values
            elif isinstance(values, bytes):
                filter = pl.col(column_name) == '0x' + values.hex()
            elif isinstance(values, list):
                if len(values) == 0:
                    filter = False
                elif isinstance(values[0], bytes):
                    filter = pl.col(column_name).is_in(
                        ['0x' + value.hex() for value in values]
                    )
                elif isinstance(values[0], str):
                    filter = pl.col(column_name).is_in(values)
                else:
                    raise Exception()
            else:
                raise Exception()

        else:
            raise Exception('column must have type pl.Binary or pl.String')

        filters.append(filter)

    return df.filter(*filters)
