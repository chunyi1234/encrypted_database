import itertools
from typing import Set

from rocksdict import Rdict

from .crypto import H1, H2
from .op import Opterator
from .utils import uint_to_bytes, xor_bytes


class FASTIOServerPart:
    def __init__(self, T_e: Rdict, T_c: Rdict) -> None:
        self.T_e = T_e
        self.T_c = T_c

    def update_server_part(self, u: bytes, e: bytes) -> None:
        self.T_e[u] = e

    def search_server_part(self, t_w: bytes, k_w: bytes | None, c: int) -> Set[bytes]:
        try:
            ID: Set[bytes] = self.T_c[t_w]
        except Exception as ex:
            if str(ex) != "key not found":
                raise ex
            ID = set()

        if k_w is None:
            return ID

        for i in range(1, c + 1):
            bytes_i = uint_to_bytes(i)
            u_i = H1(k_w + bytes_i)
            raw = xor_bytes(self.T_e[u_i], itertools.cycle(H2(k_w + bytes_i)))
            ind = raw[:-1]
            op = Opterator.from_bytes(raw[-1:])

            if op is Opterator.DEL:
                try:
                    ID.remove(ind)
                except KeyError:
                    pass
            elif op is Opterator.ADD:
                ID.add(ind)
            del self.T_e[u_i]

        self.T_c[t_w] = ID

        return ID
