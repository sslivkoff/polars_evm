from __future__ import annotations

import typing
import pytest
from polars_evm._helpers.decoding.decoding_types import parse_abi_type

if typing.TYPE_CHECKING:
    from polars_evm._helpers.decoding.decoding_types import AbiType


defaults = {
    'static': True,
    'n_bits': None,
    'fixed_scale': None,
    'array_type': None,
    'array_length': None,
    'tuple_names': None,
    'tuple_types': None,
    'has_tail': False,
}

abi_types = {
    'address': {'name': 'address', 'n_bits': 160},
    'boolean': {'name': 'boolean', 'n_bits': 8},
    'string': {'name': 'string', 'static': False, 'has_tail': True},
    'bytes': {'name': 'bytes', 'static': False, 'has_tail': True},
    'function': {'name': 'function', 'n_bits': 192},
}

# int and uint
for n_bits in range(8, 256 + 8, 8):
    name = 'int' + str(n_bits)
    abi_types[name] = {'name': name, 'n_bits': n_bits}
    name = 'uint' + str(n_bits)
    abi_types[name] = {'name': name, 'n_bits': n_bits}

# fixed and ufixed
for n_bits in range(8, 256 + 8, 8):
    for fixed_scale in range(1, 80 + 1):
        name = 'fixed' + str(n_bits) + 'x' + str(fixed_scale)
        abi_types[name] = {
            'name': name,
            'n_bits': n_bits,
            'fixed_scale': fixed_scale,
        }
        name = 'ufixed' + str(n_bits) + 'x' + str(fixed_scale)
        abi_types[name] = {
            'name': name,
            'n_bits': n_bits,
            'fixed_scale': fixed_scale,
        }

# bytes
for n_bytes in range(1, 33):
    name = 'bytes' + str(n_bytes)
    abi_types[name] = {'name': name, 'static': True, 'n_bits': n_bytes * 8}

# set defaults
for name, abi_type in list(abi_types.items()):
    abi_types[name] = defaults.copy()
    abi_types[name].update(abi_type)

# synonyms
abi_types['int'] = abi_types['int256']
abi_types['uint'] = abi_types['uint256']
abi_types['fixed'] = abi_types['fixed128x18']
abi_types['ufixed'] = abi_types['ufixed128x18']

# arrays
for name, abi_type in list(abi_types.items()):
    array_name = name + '[]'
    abi_types[array_name] = {
        'name': array_name,
        'static': abi_type['static'],
        'array_type': abi_type,
        'has_tail': True,
    }

    array_name = name + '[4]'
    abi_types[array_name] = {
        'name': array_name,
        'static': abi_type['static'],
        'array_type': abi_type,
        'array_length': 4,
        'has_tail': True,
    }

# tuples
# raise NotImplementedError()


# set defaults
for name, abi_type in list(abi_types.items()):
    abi_types[name] = defaults.copy()
    abi_types[name].update(abi_type)


@pytest.mark.parametrize('abi_type_test', list(abi_types.items()))
def test_abi_type_parsing(abi_type_test: tuple[str, AbiType]) -> None:
    abi_type, target_parsed = abi_type_test
    actual_parsed = parse_abi_type(abi_type)
    assert target_parsed == actual_parsed
