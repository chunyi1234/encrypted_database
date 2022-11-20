from os import urandom

from pytest import TempPathFactory

from encrypted_database.storage import RdictStorage

from .client import PointIndexClientPart
from .server import PointIndexServerPart


def test_point_index(tmp_path_factory: TempPathFactory):
    dir = tmp_path_factory.mktemp("data")
    client_storage = RdictStorage(str(dir / "client_dict"))
    client_sub_storage = client_storage.get_namespace("index1")
    key = urandom(16)
    server_storage = RdictStorage(str(dir / "server_dict"))
    server_sub_storage = server_storage.get_namespace("index1")

    index_client_part = PointIndexClientPart(client_sub_storage, key, new=True)
    index_server_part = PointIndexServerPart(server_sub_storage, new=True)

    msg = index_client_part.gen_insert_msg(b"123456", "hello")
    index_server_part.update_by_msg(msg)

    msg2 = index_client_part.gen_search_msg("hello")
    assert msg2 is not None
    result = index_server_part.search_by_msg(msg2)
    assert result == {b"123456"}

    client_sub_storage.close()
    client_storage.close()
    server_sub_storage.close()
    server_storage.close()

    client_storage = RdictStorage(str(dir / "client_dict"))
    client_sub_storage = client_storage.get_namespace("index1")
    server_storage = RdictStorage(str(dir / "server_dict"))
    server_sub_storage = server_storage.get_namespace("index1")

    index_client_part = PointIndexClientPart(client_sub_storage, key)
    index_server_part = PointIndexServerPart(server_sub_storage)

    msg2 = index_client_part.gen_search_msg("hello")
    assert msg2 is not None
    result = index_server_part.search_by_msg(msg2)
    assert result == {b"123456"}
