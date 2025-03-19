from __future__ import annotations

import typing
import pytest
import itertools
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
    'bool': {'name': 'bool', 'n_bits': 8},
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
        'static': False,
        'array_type': abi_type,
        'has_tail': True,
    }

    array_name = name + '[4]'
    if abi_type['n_bits'] is not None:
        n_bits = abi_type['n_bits'] * 4
    else:
        n_bits = None
    abi_types[array_name] = {
        'name': array_name,
        'static': abi_type['static'],
        'array_type': abi_type,
        'array_length': 4,
        'n_bits': n_bits,
        'has_tail': True,
    }

# set defaults
for name, abi_type in list(abi_types.items()):
    abi_types[name] = defaults.copy()
    abi_types[name].update(abi_type)

# tuples
piece_sets = [
    ('int32',),
    ('int32', 'int64'),
    ('int32', 'int64[]', 'bytes32'),
]
for subtypes in piece_sets:
    combos = itertools.product(
        *[[None, 'name' + str(i)] for i in range(len(subtypes))]
    )
    for names in combos:
        tuple_name = '('
        for subname, subtype in zip(names, subtypes):
            if len(tuple_name) > 1:
                tuple_name += ','
            if subname is None:
                tuple_name += subtype
            else:
                tuple_name += subtype + ' ' + subname
        tuple_name += ')'
        tuple_types = [abi_types[subtype] for subtype in subtypes]

        static = all(tuple_type['static'] for tuple_type in tuple_types)
        if static:
            n_bits = sum(tuple_type['n_bits'] for tuple_type in tuple_types)
        else:
            n_bits = None

        abi_types[tuple_name] = {
            'name': tuple_name,
            'static': static,
            'n_bits': n_bits,
            'has_tail': True,
            'tuple_names': list(names),
            'tuple_types': tuple_types,
        }


# set defaults
for name, abi_type in list(abi_types.items()):
    abi_types[name] = defaults.copy()
    abi_types[name].update(abi_type)


@pytest.mark.parametrize('abi_type_test', list(abi_types.items()))
def test_abi_type_parsing(abi_type_test: tuple[str, AbiType]) -> None:
    abi_type, target_parsed = abi_type_test
    actual_parsed = parse_abi_type(abi_type)
    assert target_parsed == actual_parsed
