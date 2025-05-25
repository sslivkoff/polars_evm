from __future__ import annotations

import polars as pl


def serialize_expr_dict(exprs: dict[str, pl.Expr]) -> str:
    import base64
    import json

    # serialized_dict = {
    #     key: expr.meta.serialize() for key, expr in exprs.items()
    # }
    # return json.dumps(serialized_dict)
    serialized_dict = {
        key: base64.b64encode(expr.meta.serialize()).decode('utf-8')
        for key, expr in exprs.items()
    }
    return json.dumps(serialized_dict)


def deserialize_expr_dict(exprs: str) -> dict[str, pl.Expr]:
    import base64
    import json

    serialized_dict = json.loads(exprs)
    return {
        key: pl.Expr.deserialize(base64.b64decode(value))
        for key, value in serialized_dict.items()
    }

    # serialized_dict = json.loads(exprs)
    # return {
    #     key: pl.Expr.deserialize(value)
    #     for key, value in serialized_dict.items()
    # }
