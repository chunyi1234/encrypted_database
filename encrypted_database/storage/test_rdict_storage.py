import pytest

from .rdict_storage import RdictStorage


def test_rdict_storage(tmp_path_factory: pytest.TempPathFactory):
    dir = tmp_path_factory.mktemp("data")
    storage = RdictStorage(str(dir / "hello"))

    map1 = storage.get_map("hhh")
    map1["1"] = 4

    sub_storage = storage.get_namespace("sub")

    map2 = sub_storage.get_map("xxx")
    map2["5"] = 7

    sub_storage.close()
    storage.close()

    storage = RdictStorage(str(dir / "hello"))

    map1 = storage.get_map("hhh")
    assert map1["1"] == 4

    sub_storage = storage.get_namespace("sub")

    map2 = sub_storage.get_map("xxx")
    assert map2["5"] == 7
