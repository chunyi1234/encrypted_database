from os import urandom
from typing import Any, Dict, List

from .utils import build_namespace, dict_merge

DEFAULT_COLUMN_INFO: Dict[str, dict] = {
    "point_index": {
        "enable": False,
    },
    "range_index": {
        "enable": False,
        "m": 64,
    },
}

DEFAULT_COLUMN_INFO_FOR_SERVER: Dict[str, dict] = {
    "point_index": {
        "enable": False,
    },
    "range_index": {
        "enable": False,
    },
}


class ColumnClientPart:
    def __init__(self, namespace_prefixs: List[str]) -> None:
        self._namespace_prefixs = namespace_prefixs
        self._point_index: Dict[str, Any] | None = None
        self._range_index: Dict[str, Any] | None = None
        self._key = urandom(16)

    @classmethod
    def from_dict(cls, namespace_prefixs: List[str], column_info: dict):
        column = cls(namespace_prefixs)
        column_info = dict_merge(DEFAULT_COLUMN_INFO, column_info)
        if column_info["point_index"]["enable"]:
            column.set_point_index()
        if column_info["range_index"]["enable"]:
            column.set_range_index(column_info["range_index"]["m"])
        return column

    def set_namespace_prefixs(self, namespace_prefixs: List[str]):
        self._namespace_prefixs = namespace_prefixs

    def namespace_prefixs(self) -> List[str]:
        return self._namespace_prefixs

    def key(self) -> bytes:
        return self._key

    def set_point_index(self):
        self._point_index = {
            "namespace": build_namespace(self._namespace_prefixs + ["point"]),
            "key": urandom(16),
        }

    def has_point_index(self) -> bool:
        return self._point_index is not None

    def point_index(self):
        return self._point_index

    def set_range_index(self, m: int):
        self._range_index = {
            "namespace": build_namespace(self._namespace_prefixs + ["range"]),
            "m": m,
            "key": urandom(16),
        }

    def has_range_index(self) -> bool:
        return self._range_index is not None

    def range_index(self):
        return self._range_index


class ColumnServerPart:
    def __init__(self, namespace_prefixs: List[str]) -> None:
        self._namespace_prefixs = namespace_prefixs
        self._point_index: Dict[str, Any] | None = None
        self._range_index: Dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, namespace_prefix: List[str], column_info: dict):
        column = cls(namespace_prefix)
        column_info = dict_merge(DEFAULT_COLUMN_INFO_FOR_SERVER, column_info)
        if column_info["point_index"]["enable"]:
            column.set_point_index()
        if column_info["range_index"]["enable"]:
            column.set_range_index()
        return column

    def set_namespace_prefixs(self, namespace_prefixs: List[str]):
        self._namespace_prefixs = namespace_prefixs

    def namespace_prefix(self) -> List[str]:
        return self._namespace_prefixs

    def set_point_index(self):
        self._point_index = {
            "namespace": build_namespace(self._namespace_prefixs + [":point"])
        }

    def has_point_index(self) -> bool:
        return self._point_index is not None

    def point_index(self):
        return self._point_index

    def set_range_index(self):
        self._range_index = {
            "namespace": build_namespace(self._namespace_prefixs + ["range"])
        }

    def has_range_index(self) -> bool:
        return self._range_index is not None

    def range_index(self):
        return self._range_index
