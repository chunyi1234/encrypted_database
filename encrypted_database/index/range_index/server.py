import pickle
from typing import Set

from rocksdict import Rdict

from ..fastio import FASTIOServerPart


class RangeIndexServerPart:
    def __init__(self, storage: Rdict, cf_prefix: str, new: bool) -> None:
        if new:
            self.T_e = storage.create_column_family(cf_prefix + ":" + "T_e")
            self.T_c = storage.create_column_family(cf_prefix + ":" + "T_c")
        else:
            self.T_e = storage.get_column_family(cf_prefix + ":" + "T_e")
            self.T_c = storage.get_column_family(cf_prefix + ":" + "T_c")
        self.fastio_server = FASTIOServerPart(self.T_e, self.T_c)

    def update_by_msg(self, msg: bytes):
        tokens = pickle.loads(msg)

        for u, e in tokens:
            self.fastio_server.update_server_part(u, e)

    def search_by_msg(self, msg: bytes) -> Set[bytes]:
        tokens = pickle.loads(msg)

        result: Set[bytes] = set()
        for t_w, k_w, c in tokens:
            result = result.union(self.fastio_server.search_server_part(t_w, k_w, c))

        return result

    def close(self):
        self.T_e.close()
        self.T_c.close()
