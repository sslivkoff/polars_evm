
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

series = pl.Series('address', addresses)
df = pl.DataFrame(series)

print('hex series:', series.evm.binary_to_hex())
print('hex df:', df.evm.binary_to_hex())
print('hex expr:', df.select(pl.col('address').evm.binary_to_hex()))
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

# LazyFrame namespace
lf.evm.binary_to_hex(prefix=True, columns=None)
lf.evm.hex_to_binary(prefix=True, columns=None)

# Series namespace
series.evm.binary_to_hex(prefix=True)
series.evm.hex_to_binary(prefix=True)
series.evm.keccak(output='hex', text=False)

# Expression namespace
pl.Expr.evm.binary_to_hex(prefix=True)
pl.Expr.evm.hex_to_binary(prefix=True)
pl.Expr.evm.keccak(output='hex', text=False)
```

## Additional utilities

Beyond the `evm` namespace, `polars_evm` has the following utilities:
- `set_column_display_width()`: set display width so that it fully displays tx hashes in jupyter notebooks and other printouts

## TODO
- use efficient rust implementations where possible
- abi encoding
- rlp encoding
