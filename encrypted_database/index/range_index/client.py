import pickle

from rocksdict import Rdict

from ..fastio import FASTIOClientPart, Opterator
from .utils import get_bin_prefixs, get_BRC


class RangeIndexClientPart:
    def __init__(
        self, storage: Rdict, cf_prefix: str, key: bytes, m: int, new: bool
    ) -> None:
        if new:
            self.Sigma = storage.create_column_family(cf_prefix + ":" + "Sigma")
        else:
            self.Sigma = storage.get_column_family(cf_prefix + ":" + "Sigma")
        self.fastio_client = FASTIOClientPart(self.Sigma, key)
        self.m = m

    def gen_insert_msg(self, ind: bytes, value: int) -> bytes:
        msgs = []
        for prefix in get_bin_prefixs(self.m, value):
            msgs.append(
                self.fastio_client.update_client_part(
                    ind, pickle.dumps(prefix), Opterator.ADD
                )
            )

        return pickle.dumps(msgs)

    def gen_delete_msg(self, ind: bytes, value: int) -> bytes:
        msgs = []
        for prefix in get_bin_prefixs(self.m, value):
            msgs.append(
                self.fastio_client.update_client_part(
                    ind, pickle.dumps(prefix), Opterator.DEL
                )
            )

        return pickle.dumps(msgs)

    def gen_update_msg(self, a: int, b: int) -> bytes:
        msgs = []
        for node in get_BRC(self.m, a, b):
            msg = self.fastio_client.search_client_part(pickle.dumps(node))
            if msg is not None:
                msgs.append(msg)

        return pickle.dumps(msgs)

    def close(self) -> None:
        self.Sigma.close()
