import itertools
from typing import Iterable


def uint_to_bytes(n: int) -> bytes:
    return n.to_bytes(8, byteorder="big", signed=False)


def xor_bytes(a: Iterable[int], b: Iterable[int]) -> bytes:
    return bytes(itertools.starmap(lambda a, b: a ^ b, zip(a, b)))
