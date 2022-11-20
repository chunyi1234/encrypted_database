import pickle

from encrypted_database.storage import Storage

from ..fastio import FASTIOClientPart, Opterator


class PointIndexClientPart:
    def __init__(self, storage: Storage, key: bytes, new: bool) -> None:
        if new:
            Sigma = storage.create_map("Sigma")
        else:
            Sigma = storage.get_map("Sigma")
        self.fastio_client = FASTIOClientPart(Sigma, key)

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
