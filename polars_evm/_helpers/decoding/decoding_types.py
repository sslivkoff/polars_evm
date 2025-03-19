from __future__ import annotations

import typing

if typing.TYPE_CHECKING:

    class AbiType(typing.TypedDict):
        name: str
        static: bool
        n_bits: int | None
        fixed_scale: int | None
        array_type: AbiType | None
        array_length: int | None
        tuple_names: list[str | None] | None
        tuple_types: list[AbiType] | None
        has_tail: bool


def parse_abi_type(abi_type: str) -> AbiType:
    # default values
    static = True
    n_bits = None
    fixed_scale = None
    array_type = None
    array_length = None
    tuple_names = None
    tuple_types = None
    has_tail = False

    # replace known synonyms
    if abi_type == 'int':
        abi_type = 'int256'
    elif abi_type == 'uint':
        abi_type = 'uint256'
    elif abi_type == 'fixed':
        abi_type = 'fixed128x18'
    elif abi_type == 'ufixed':
        abi_type = 'ufixed128x18'

    # parse by type
    if abi_type.endswith(']'):
        has_tail = True
        if not abi_type.endswith('[]'):
            array_length = int(abi_type.rsplit('[', maxsplit=1)[1][:-1])
        array_type = parse_abi_type(abi_type.rsplit('[', maxsplit=1)[0])
        static = array_type['static']
    elif abi_type.endswith(')'):
        tuple_names, tuple_types = _parse_tuple_type(abi_type)
        static = all(subtype['static'] for subtype in tuple_types)
        has_tail = True
    elif abi_type == 'bytes':
        static = False
        has_tail = True
    elif abi_type == 'string':
        static = False
        has_tail = True
    elif abi_type == 'address':
        n_bits = 160
    elif abi_type == 'bool':
        n_bits = 8
    elif abi_type.startswith('int'):
        n_bits = int(abi_type[3:])
    elif abi_type.startswith('uint'):
        n_bits = int(abi_type[4:])
    elif abi_type.startswith('bytes'):
        n_bits = int(abi_type[5:]) * 8
    elif abi_type.startswith('fixed'):
        n_bits_str, fixed_scale_str = abi_type[5:].split('x')
        n_bits = int(n_bits_str)
        fixed_scale = int(fixed_scale_str)
    elif abi_type.startswith('ufixed'):
        n_bits_str, fixed_scale_str = abi_type[6:].split('x')
        n_bits = int(n_bits_str)
        fixed_scale = int(fixed_scale_str)
    elif abi_type == 'function':
        n_bits = 192
    else:
        raise Exception('invalid abi type: ' + str(abi_type))

    return {
        'name': abi_type,
        'static': static,
        'n_bits': n_bits,
        'fixed_scale': fixed_scale,
        'array_type': array_type,
        'array_length': array_length,
        'tuple_names': tuple_names,
        'tuple_types': tuple_types,
        'has_tail': has_tail,
    }


def _parse_tuple_type(
    abi_type: str,
) -> tuple[list[str | None] | None, list[AbiType]]:
    raw_pieces = abi_type[1:-1].split(',')

    # join pieces of nested tuples
    pieces = []
    searching = False
    search_start = None
    search_sum = 0
    p = 0
    while p < len(raw_pieces):
        imbalance = raw_pieces[p].count('(') - raw_pieces[p].count(')')
        if searching:
            search_sum += imbalance
            if search_sum == 0:
                searching = False
                pieces.append(''.join(raw_pieces[search_start : (p + 1)]))
        else:
            if imbalance > 0:
                searching = True
                search_sum = imbalance
                search_start = p
            else:
                pieces.append(raw_pieces[p])
        p += 1

    # parse each piece as a subtype with optional name
    tuple_names = []
    tuple_types = []
    for piece in pieces:
        name = None
        subtype = piece
        if ' ' in piece:
            head, tail = piece.rsplit(' ', maxsplit=1)
            if ']' not in tail and '(' not in tail:
                name = tail
                subtype = head
        tuple_names.append(name)
        tuple_types.append(parse_abi_type(subtype))

    return tuple_names, tuple_types
