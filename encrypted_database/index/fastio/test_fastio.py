import os

import pytest

from encrypted_database.storage import RdictStorage

from . import FASTIOClientPart, FASTIOServerPart, Opterator


def test_fastio(tmp_path_factory: pytest.TempPathFactory):
    dir = tmp_path_factory.mktemp("data")
    client_storage = RdictStorage(str(dir / "client_dict"))
    Sigma = client_storage.create_map("Sigma")
    server_storage = RdictStorage(str(dir / "server_dict"))
    T_e = server_storage.create_map("T_e")
    T_c = server_storage.create_map("T_c")

    k_s = os.urandom(16)
    client = FASTIOClientPart(Sigma, k_s)
    server = FASTIOServerPart(T_e, T_c)

    u, e = client.update_client_part(b"111", b"hello", Opterator.ADD)
    server.update_server_part(u, e)

    scp = client.search_client_part(b"hello")
    assert scp is not None
    t_w, k_w, c = scp
    result = server.search_server_part(t_w, k_w, c)
    assert result == {b"111"}

    scp = client.search_client_part(b"hello")
    assert scp is not None
    t_w, k_w, c = scp
    result = server.search_server_part(t_w, k_w, c)
    assert result == {b"111"}

    scp = client.search_client_part(b"warn")
    assert scp is None

    client_storage.close()
    server_storage.close()

    client_storage = RdictStorage(str(dir / "client_dict"))
    Sigma = client_storage.get_map("Sigma")
    server_storage = RdictStorage(str(dir / "server_dict"))
    T_e = server_storage.get_map("T_e")
    T_c = server_storage.get_map("T_c")

    client = FASTIOClientPart(Sigma, k_s)
    server = FASTIOServerPart(T_e, T_c)

    scp = client.search_client_part(b"hello")
    assert scp is not None
    t_w, k_w, c = scp
    result = server.search_server_part(t_w, k_w, c)
    assert result == {b"111"}

    scp = client.search_client_part(b"hello")
    assert scp is not None
    t_w, k_w, c = scp
    result = server.search_server_part(t_w, k_w, c)
    assert result == {b"111"}

    scp = client.search_client_part(b"warn")
    assert scp is None

    u, e = client.update_client_part(b"111", b"hello", Opterator.DEL)
    server.update_server_part(u, e)

    scp = client.search_client_part(b"hello")
    assert scp is not None
    t_w, k_w, c = scp
    result = server.search_server_part(t_w, k_w, c)
    assert result == set()
