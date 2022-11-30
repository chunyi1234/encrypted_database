from typing import Set

from encrypted_database.storage import Storage

from .schema import SchemaClientPart, SchemaServerPart


class MetaDataClientPart:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.schemas = storage.get_map("schemas")
        self.metadata = storage.get_map("metadata")
        if "tables" not in self.metadata.keys():
            self.metadata["tables"] = set()

    def table_names(self) -> Set[str]:
        return self.metadata["tables"]

    def _add_table_name(self, name: str):
        tables = self.metadata["tables"]
        tables.add(name)
        self.metadata["tables"] = tables

    def create_table_schema(self, name: str, schema_info: dict) -> SchemaClientPart:
        if name in self.table_names():
            raise Exception("table already exists")
        self._add_table_name(name)
        schema = SchemaClientPart.from_dict([name], schema_info)
        self.schemas[name] = schema

        return schema

    def table_schema(self, name: str) -> SchemaClientPart:
        return self.schemas[name]

    def del_table_schema(self, name: str):
        del self.schemas[name]


class MetaDataServerPart:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.schemas = storage.get_map("schemas")
        self.metadata = storage.get_map("metadata")
        if "tables" not in self.metadata.keys():
            self.metadata["tables"] = set()

    def table_names(self) -> Set[str]:
        return self.metadata["tables"]

    def _add_table_name(self, name: str):
        tables = self.metadata["tables"]
        tables.add(name)
        self.metadata["tables"] = tables

    def create_table_schema(self, name: str, schema_info: dict) -> SchemaServerPart:
        if name in self.table_names():
            raise Exception("table already exists")
        self._add_table_name(name)
        schema = SchemaServerPart.from_dict([name], schema_info)
        self.schemas[name] = schema

        return schema

    def table_schema(self, name: str) -> SchemaServerPart:
        return self.schemas[name]

    def del_table_schema(self, name: str):
        del self.schemas[name]
