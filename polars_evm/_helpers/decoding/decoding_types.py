from __future__ import annotations


def get_static_n_bits(abi_type: str) -> int:
    if abi_type.endswith(']'):
        raise NotImplementedError('array decoding not implemented')
    elif abi_type.endswith(')'):
        raise NotImplementedError('tuple decoding not implemented')
    elif abi_type == 'bytes':
        raise NotImplementedError('bytes')
    elif abi_type == 'address':
        return 160
    elif abi_type == 'boolean':
        return 8
    elif abi_type.startswith('int'):
        return int(abi_type[3:])
    elif abi_type.startswith('uint'):
        return int(abi_type[4:])
    elif abi_type.startswith('bytes'):
        return int(abi_type[5:]) * 8
    elif abi_type.startswith('fixed'):
        return int(abi_type[5:].split('x')[1])
    elif abi_type.startswith('ufixed'):
        return int(abi_type[6:].split('x')[1])
    elif abi_type == 'function':
        return 192
    else:
        raise Exception('invalid abi type: ' + str(abi_type))
