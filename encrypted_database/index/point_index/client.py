import pickle

from rocksdict import Rdict

from ..fastio import FASTIOClientPart, Opterator


class PointIndexClientPart:
    def __init__(self, storage: Rdict, cf_prefix: str, key: bytes, new: bool) -> None:
        if new:
            self.Sigma = storage.create_column_family(cf_prefix + ":" + "Sigma")
        else:
            self.Sigma = storage.get_column_family(cf_prefix + ":" + "Sigma")
        self.fastio_client = FASTIOClientPart(self.Sigma, key)

    def gen_insert_msg(self, ind, w) -> bytes:
        msg = self.fastio_client.update_client_part(ind, pickle.dumps(w), Opterator.ADD)

        return pickle.dumps(msg)

    def gen_delete_msg(self, ind: bytes, w) -> bytes:
        msg = self.fastio_client.update_client_part(ind, pickle.dumps(w), Opterator.DEL)

        return pickle.dumps(msg)

    def gen_search_msg(self, w) -> bytes | None:
        msg = self.fastio_client.search_client_part(w)

        if msg is None:
            return None
        else:
            return pickle.dumps(msg)

    def close(self) -> None:
        self.Sigma.close()
