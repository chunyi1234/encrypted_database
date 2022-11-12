from .utils import get_bin_prefixs, get_BRC


def test_get_BRC():
    result = get_BRC(3, 2, 7)

    assert result == {(0b01, 1), (0b1, 2)}


def test_get_BRC2():
    result = get_BRC(3, 0, 7)

    assert result == {(0, 3)}


def test_get_bin_prefixs():
    result = get_bin_prefixs(3, 6)

    assert result == [(0b110, 0), (0b11, 1), (0b1, 2), (0, 3)]
