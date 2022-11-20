from base64 import b64encode
from copy import copy
from typing import Any, Dict, List

from rocksdict import Rdict

from .storage import Key, Map, Storage


def str_encode(input: str) -> str:
    return b64encode(input.encode("UTF-8")).decode("UTF-8")


class RdictStorage(Storage):
    def __init__(self, path: str) -> None:
        self._rdict = Rdict(path)
        self._namespaces: List[str] = []
        self._cfs: Dict[str, Rdict] = {}
        self._prefix = ""

    def get_namespace(self, namespace: str):
        if namespace in self._namespaces:
            raise Exception("namespace already exists")
        sub_storage = copy(self)
        sub_storage._namespaces = []
        sub_storage._cfs = {}
        sub_storage._prefix += ":" + str_encode(namespace)
        self._namespaces.append(namespace)
        return sub_storage

    def namespaces(self) -> List[str]:
        return self._namespaces

    def names(self) -> List[str]:
        return list(self._cfs.keys())

    def create_map(self, name: str) -> Map:
        cf_name = self._prefix + ":" + str_encode(name)
        rdict = self._rdict.create_column_family(cf_name)
        self._cfs[name] = rdict
        return RdictMap(rdict)

    def get_map(self, name: str) -> Map:
        if name in self._cfs.keys():
            return self._cfs[name]
        cf_name = self._prefix + ":" + str_encode(name)
        rdict = self._rdict.get_column_family(cf_name)
        self._cfs[name] = rdict
        return RdictMap(rdict)

    def drop_map(self, name: str) -> None:
        if name in self._cfs.keys():
            self._cfs[name].close()
            del self._cfs[name]
        cf_name = self._prefix + ":" + str_encode(name)
        self._rdict.drop_column_family(cf_name)

    def close(self) -> None:
        for cf in self._cfs.values():
            cf.close()
        if self._prefix == "":
            self._rdict.close()


class RdictMap(Map):
    def __init__(self, rdict: Rdict) -> None:
        self._rdict = rdict

    def __setitem__(self, key: Key, item: Any):
        self._rdict[key] = item

    def __getitem__(self, key: Key) -> Any:
        return self._rdict[key]

    def __delitem__(self, key: Key):
        del self._rdict[key]

    def __contains__(self, key: Key) -> bool:
        return key in self._rdict
