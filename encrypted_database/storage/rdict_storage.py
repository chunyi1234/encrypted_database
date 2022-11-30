from base64 import b64encode
from copy import copy
from typing import Any, Dict, List

from rocksdict import Rdict

from .storage import Key, Map, Storage


def str_encode(input: str) -> str:
    return b64encode(input.encode("UTF-8")).decode("UTF-8")


class RdictStorage(Storage):
    def __init__(self, path: str) -> None:
        try:
            self._cf_names = Rdict.list_cf(path)
        except Exception:
            self._cf_names = []
        self._rdict = Rdict(path)
        self._namespaces: List[str] = []
        self._cf_map: Dict[str, Rdict] = {}
        self._prefix = ""

    def get_namespace(self, namespace: str):
        if namespace in self._namespaces:
            raise Exception("namespace already exists")
        sub_storage = copy(self)
        sub_storage._namespaces = []
        sub_storage._cf_map = {}
        prefix_added = ":" + str_encode(namespace)
        sub_storage._prefix += prefix_added
        sub_storage._cf_names = []
        for cf_name in self._cf_names:
            if cf_name.startswith(prefix_added):
                sub_storage._cf_names.append(cf_name.removeprefix(prefix_added))
        self._namespaces.append(namespace)
        return sub_storage

    def namespaces(self) -> List[str]:
        return self._namespaces

    def names(self) -> List[str]:
        return list(self._cf_map.keys())

    def create_map(self, name: str) -> Map:
        cf_name = self._prefix + ":" + str_encode(name)
        rdict = self._rdict.create_column_family(cf_name)
        self._cf_map[name] = rdict
        return RdictMap(rdict)

    def get_map(self, name: str) -> Map:
        if name in self._cf_map.keys():
            return RdictMap(self._cf_map[name])
        cf_name_suffix = ":" + str_encode(name)
        cf_name = self._prefix + cf_name_suffix
        if cf_name_suffix in self._cf_names:
            rdict = self._rdict.get_column_family(cf_name)
        else:
            rdict = self._rdict.create_column_family(cf_name)
        self._cf_map[name] = rdict
        return RdictMap(rdict)

    def drop_map(self, name: str) -> None:
        if name in self._cf_map.keys():
            self._cf_map[name].close()
            del self._cf_map[name]
        cf_name = self._prefix + ":" + str_encode(name)
        self._rdict.drop_column_family(cf_name)

    def close(self) -> None:
        for cf in self._cf_map.values():
            cf.close()
        if self._prefix == "":
            self._rdict.close()


class RdictMap(Map):
    def __init__(self, rdict: Rdict) -> None:
        self._rdict = rdict

    def __setitem__(self, key: Key, item: Any):
        self._rdict[key] = item

    def __getitem__(self, key: Key) -> Any:
        try:
            return self._rdict[key]
        except Exception as ex:
            if str(ex) == "key not found":
                raise KeyError("key not found")
            raise ex

    def __delitem__(self, key: Key):
        del self._rdict[key]

    def __contains__(self, key: Key) -> bool:
        return key in self._rdict

    def get(self, key: Key, default=None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self) -> List[Key]:
        return list(self._rdict.keys())
