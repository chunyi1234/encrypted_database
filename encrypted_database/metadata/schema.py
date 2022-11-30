from os import urandom
from typing import Any, Dict, List

from .column import ColumnClientPart, ColumnServerPart
from .utils import build_namespace, dict_merge

DEFAULT_SCHEMA_INFO: Dict[str, Any] = {"columns": {}}

DEFAULT_SCHEMA_INFO_FOR_SERVER: Dict[str, Any] = {"columns": {}}


class SchemaClientPart:
    def __init__(self, namespace_prefixs: List[str]) -> None:
        self._namespace_prefixs = namespace_prefixs
        self._columns: Dict[str, ColumnClientPart] = {}
        self._key = urandom(16)

    @classmethod
    def from_dict(cls, namespace_prefixs: List[str], schema_info: Dict[str, Any]):
        schema_info = dict_merge(DEFAULT_SCHEMA_INFO, schema_info)
        schema = cls(namespace_prefixs)
        for name, column in schema_info["columns"].items():
            schema.set_column(name, column)
        return schema

    def key(self) -> bytes:
        return self._key

    def set_column(self, name: str, column: dict[str, Any] | ColumnClientPart):
        if isinstance(column, ColumnClientPart):
            self._set_column(name, column)
        else:
            self._set_column_from_dict(name, column)

    def _set_column_from_dict(self, name: str, column_info: dict[str, Any]):
        column = ColumnClientPart.from_dict(
            self._namespace_prefixs + ["column", name], column_info
        )
        self._columns[name] = column

    def _set_column(self, name: str, column: ColumnClientPart):
        column.set_namespace_prefixs(self._namespace_prefixs + ["column", name])
        self._columns[name] = column

    def column_names(self) -> List[str]:
        return list(self._columns.keys())

    def has_column(self, column_name: str) -> bool:
        return column_name in self._columns.keys()

    def column(self, column_name: str) -> ColumnClientPart:
        return self._columns[column_name]


class SchemaServerPart:
    def __init__(self, namespace_prefixs: List[str]) -> None:
        self._namespace_prefixs = namespace_prefixs
        self._columns: Dict[str, ColumnServerPart] = {}
        self._collection_name = build_namespace(namespace_prefixs + ["collection"])

    @classmethod
    def from_dict(cls, namespace_prefixs: List[str], schema_info: Dict[str, Any]):
        schema_info = dict_merge(DEFAULT_SCHEMA_INFO, schema_info)
        schema = cls(namespace_prefixs)
        for name, column in schema_info["columns"].items():
            schema.set_column(name, column)
        return schema

    def collection_name(self):
        return self._collection_name

    def set_column(self, name: str, column: dict[str, Any] | ColumnServerPart):
        if isinstance(column, ColumnServerPart):
            self._set_column(name, column)
        else:
            self._set_column_from_dict(name, column)

    def _set_column_from_dict(self, name: str, column_info: dict[str, Any]):
        column = ColumnServerPart.from_dict(
            self._namespace_prefixs + ["column", name], column_info
        )
        self._columns[name] = column

    def _set_column(self, name: str, column: ColumnServerPart):
        column.set_namespace_prefixs(self._namespace_prefixs + ["column", name])
        self._columns[name] = column

    def column_names(self) -> List[str]:
        return list(self._columns.keys())

    def has_column(self, column_name: str) -> bool:
        return column_name in self._columns.keys()

    def column(self, column_name: str) -> ColumnServerPart:
        return self._columns[column_name]
