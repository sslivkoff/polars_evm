
# Polars EVM

Polars EVM adds the `evm` namespace to polars on dataframes, lazyframes, series, and expressions.

This namespace has lots of functions for processing EVM data:
- binary ↔ hex conversions
- keccak
- abi encoding [TODO]
- rlp encoding [TODO]

## Installation

`pip install polars_evm`

## Usage

Just import `polars_evm` and then the `evm` namespace will be registered to polars.

```python
import polars as pl
import polars_evm

addresses = [
    b'\xda\xc1\x7f\x95\x8d.\xe5#\xa2 b\x06\x99E\x97\xc1=\x83\x1e\xc7',
    b'\xa0\xb8i\x91\xc6!\x8b6\xc1\xd1\x9dJ.\x9e\xb0\xce6\x06\xebH',
    b'_\x98\x80ZN\x8b\xe2U\xa3(\x80\xfd\xec\x7fg(\xc6V\x8b\xa0'
]

balances = [
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Jy\xb0\x9aq\x1e\xd1(",
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xd1\xff\xf7\xfb\xa8O\x87",
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01cEx]\x8a\x00\x00",
]

df = pl.DataFrame({'address': addresses, 'balance': balances})

print('converted dataframe:')
print(df.evm.binary_to_float({'balance': 'u256'}).evm.binary_to_hex())
print()
print('using expressions:')
print(df.select(pl.col.address.evm.binary_to_hex(), pl.col.balance.evm.binary_to_float('u256')))
print()
print('using series:')
print('hex series:', df['address'].evm.binary_to_hex())
print('float series:', df['balance'].evm.binary_to_float('u256'))
```

output:
```bash
hex series: shape: (3,)
Series: 'address' [str]
[
        "0xdac17f958d2e…
        "0xa0b86991c621…
        "0x5f98805a4e8b…
]
hex df: shape: (3, 1)
┌───────────────────────────────────┐
│ address                           │
│ ---                               │
│ str                               │
╞═══════════════════════════════════╡
│ 0xdac17f958d2ee523a2206206994597… │
│ 0xa0b86991c6218b36c1d19d4a2e9eb0… │
│ 0x5f98805a4e8be255a32880fdec7f67… │
└───────────────────────────────────┘
hex expr: shape: (3, 1)
┌───────────────────────────────────┐
│ literal                           │
│ ---                               │
│ str                               │
╞═══════════════════════════════════╡
│ 0xdac17f958d2ee523a2206206994597… │
│ 0xa0b86991c6218b36c1d19d4a2e9eb0… │
│ 0x5f98805a4e8be255a32880fdec7f67… │
└───────────────────────────────────┘
```

## List of namespace entries

```python
# DataFrame namespace
df.evm.binary_to_hex(prefix=True, columns=None)
df.evm.hex_to_binary(prefix=True, columns=None)
df.evm.binary_to_float({'column1': 'u256', 'column2': 'i256'}, replace=False, prefix=True)

# LazyFrame namespace
lf.evm.binary_to_hex(prefix=True, columns=None)
lf.evm.hex_to_binary(prefix=True, columns=None)
lf.evm.binary_to_float({'column1': 'u256', 'column2': 'i256'}, replace=False, prefix=True)

# Series namespace
series.evm.binary_to_hex(prefix=True)
series.evm.hex_to_binary(prefix=True)
series.evm.binary_to_float('u256')
series.evm.keccak(output='hex', text=False)

# Expression namespace
pl.Expr.evm.binary_to_hex(prefix=True)
pl.Expr.evm.hex_to_binary(prefix=True)
pl.Expr.binary_to_float('u256')
pl.Expr.evm.keccak(output='hex', text=False)
```

## Additional utilities

Beyond the `evm` namespace, `polars_evm` has the following utilities:
- `set_column_display_width()`: set display width so that it fully displays tx hashes in jupyter notebooks and other printouts

## TODO
- use efficient rust implementations where possible
- abi encoding
- rlp encoding
