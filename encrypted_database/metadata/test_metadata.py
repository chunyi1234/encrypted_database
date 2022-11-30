from pytest import TempPathFactory

from encrypted_database.storage import RdictStorage

from .metadata import MetaDataClientPart, MetaDataServerPart


def test_client_part(tmp_path_factory: TempPathFactory):
    dir = tmp_path_factory.mktemp("data")
    client_store = RdictStorage(str(dir / "client_metadata"))
    client_metadata = MetaDataClientPart(client_store)

    schema_info = {
        "columns": {
            "name": {"point_index": {"enable": True}},
            "age": {"point_index": {"enable": True}, "range_index": {"enable": True}},
        }
    }

    client_metadata.create_table_schema("people", schema_info)

    client_store.close()

    client_store = RdictStorage(str(dir / "client_metadata"))
    client_metadata = MetaDataClientPart(client_store)

    assert set(client_metadata.table_names()) == {"people"}

    schema = client_metadata.table_schema("people")
    assert set(schema.column_names()) == {"name", "age"}
    assert schema.has_column("name")
    assert schema.has_column("age")
    assert not schema.has_column("xxx")

    age_column = schema.column("age")
    assert age_column.has_point_index()
    point_index = age_column.point_index()
    assert point_index is not None
    assert set(point_index.keys()) == {"namespace", "key"}
    assert age_column.has_range_index()
    range_index = age_column.range_index()
    assert range_index is not None
    assert set(range_index.keys()) == {"namespace", "key", "m"}

    name_column = schema.column("name")
    assert name_column.has_point_index()
    point_index = name_column.point_index()
    assert point_index is not None
    assert set(point_index.keys()) == {"namespace", "key"}
    assert not name_column.has_range_index()
    range_index = name_column.range_index()
    assert range_index is None


def test_server_part(tmp_path_factory: TempPathFactory):
    dir = tmp_path_factory.mktemp("data")
    server_store = RdictStorage(str(dir / "server_metadata"))
    server_metadata = MetaDataServerPart(server_store)

    schema_info = {
        "columns": {
            "enc(name)": {"point_index": {"enable": True}},
            "enc(age)": {
                "point_index": {"enable": True},
                "range_index": {"enable": True},
            },
        }
    }

    server_metadata.create_table_schema("enc(people)", schema_info)

    server_store.close()

    server_store = RdictStorage(str(dir / "server_metadata"))
    server_metadata = MetaDataServerPart(server_store)

    assert set(server_metadata.table_names()) == {"enc(people)"}

    schema = server_metadata.table_schema("enc(people)")
    assert set(schema.column_names()) == {"enc(name)", "enc(age)"}
    assert schema.has_column("enc(name)")
    assert schema.has_column("enc(age)")
    assert not schema.has_column("xxx")

    age_column = schema.column("enc(age)")
    assert age_column.has_point_index()
    point_index = age_column.point_index()
    assert point_index is not None
    assert set(point_index.keys()) == {"namespace"}
    assert age_column.has_range_index()
    range_index = age_column.range_index()
    assert range_index is not None
    assert set(range_index.keys()) == {"namespace"}

    name_column = schema.column("enc(name)")
    assert name_column.has_point_index()
    point_index = name_column.point_index()
    assert point_index is not None
    assert set(point_index.keys()) == {"namespace"}
    assert not name_column.has_range_index()
    range_index = name_column.range_index()
    assert range_index is None
