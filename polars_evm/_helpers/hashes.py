from __future__ import annotations

import typing


def keccak(
    data: str | bytes,
    output: typing.Literal['hex', 'binary', 'prefix_hex', 'raw_hex'] = 'hex',
    text: bool = False,
) -> str | bytes:
    from Crypto.Hash import keccak as f_keccak

    # encode text as bytes
    if text:
        if isinstance(data, str):
            data = data.encode()
        else:
            raise Exception('not str, cannot use text=True')

    # decode hex into bytes
    if isinstance(data, str):
        try:
            if data.startswith('0x'):
                data = data[2:]
            data = bytes.fromhex(data)
        except ValueError:
            raise Exception('for text data, use text=True')

    # perform hash
    as_binary = f_keccak.new(digest_bits=256, data=data).digest()

    # convert to output format
    if output == 'binary':
        return as_binary
    elif output == 'raw_hex':
        return as_binary.hex()
    elif output in ['hex', 'prefix_hex']:
        return '0x' + as_binary.hex()
    else:
        raise Exception('unknown output format: ' + str(output))
