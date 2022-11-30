from base64 import b64encode
from collections.abc import Mapping
from typing import List


def dict_merge(dct, merge_dct, add_keys=True):
    dct = dct.copy()
    if not add_keys:
        merge_dct = {k: merge_dct[k] for k in set(dct).intersection(set(merge_dct))}

    for k, v in merge_dct.items():
        if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], Mapping):
            dct[k] = dict_merge(dct[k], merge_dct[k], add_keys=add_keys)
        else:
            dct[k] = merge_dct[k]

    return dct


def str_encode(input: str) -> str:
    return b64encode(input.encode("UTF-8")).decode("UTF-8")


def build_namespace(parts: List[str]) -> str:
    return ":".join([str_encode(part) for part in parts])
