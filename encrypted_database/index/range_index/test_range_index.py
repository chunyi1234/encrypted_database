from os import urandom

from pytest import TempPathFactory

from encrypted_database.storage import RdictStorage

from .client import RangeIndexClientPart
from .server import RangeIndexServerPart


def test_range_index(tmp_path_factory: TempPathFactory):
    dir = tmp_path_factory.mktemp("data")
    client_storage = RdictStorage(str(dir / "client_dict"))
    client_sub_storage = client_storage.get_namespace("index1")
    key = urandom(16)
    server_storage = RdictStorage(str(dir / "server_dict"))
    server_sub_storage = server_storage.get_namespace("index1")

    index_client_part = RangeIndexClientPart(client_sub_storage, key, 3, new=True)
    index_server_part = RangeIndexServerPart(server_sub_storage, new=True)

    msg = index_client_part.gen_insert_msg(b"123456", 5)
    index_server_part.update_by_msg(msg)

    msg = index_client_part.gen_insert_msg(b"1256", 1)
    index_server_part.update_by_msg(msg)

    msg2 = index_client_part.gen_search_msg(2, 7)
    assert msg2 is not None
    result = index_server_part.search_by_msg(msg2)
    assert result == {b"123456"}

    msg2 = index_client_part.gen_search_msg(0, 7)
    assert msg2 is not None
    result = index_server_part.search_by_msg(msg2)
    assert result == {b"123456", b"1256"}

    client_sub_storage.close()
    client_storage.close()
    server_sub_storage.close()
    server_storage.close()

    client_storage = RdictStorage(str(dir / "client_dict"))
    client_sub_storage = client_storage.get_namespace("index1")
    server_storage = RdictStorage(str(dir / "server_dict"))
    server_sub_storage = server_storage.get_namespace("index1")

    index_client_part = RangeIndexClientPart(client_sub_storage, key, 3)
    index_server_part = RangeIndexServerPart(server_sub_storage)

    msg2 = index_client_part.gen_search_msg(2, 7)
    assert msg2 is not None
    result = index_server_part.search_by_msg(msg2)
    assert result == {b"123456"}

    msg2 = index_client_part.gen_search_msg(0, 7)
    assert msg2 is not None
    result = index_server_part.search_by_msg(msg2)
    assert result == {b"123456", b"1256"}
