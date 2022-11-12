from typing import List, Set, Tuple


def get_BRC(m: int, a: int, b: int) -> Set[Tuple[int, int]]:
    BRC = set()
    i = 0
    while a < b:
        if a & 1 == 1:
            BRC.add((a, i))
        if b & 1 == 0:
            BRC.add((b, i))
        a = (a + 1) >> 1
        b = (b - 1) >> 1
        i += 1
    if a == b:
        BRC.add((a, i))

    return BRC


def get_bin_prefixs(m: int, x: int) -> List[Tuple[int, int]]:
    result = []

    for i in range(0, m + 1):
        result.append((x >> i, i))

    return result
