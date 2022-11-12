import itertools
import os
from typing import Tuple

from rocksdict import Rdict

from .crypto import H1, H2, F, h
from .op import Opterator
from .utils import uint_to_bytes, xor_bytes


class FASTIOClientPart:
    def __init__(self, Sigma: Rdict, k_s: bytes) -> None:
        self.k_s = k_s
        self.Sigma = Sigma

    def update_client_part(
        self, ind: bytes, w: bytes, op: Opterator
    ) -> Tuple[bytes, bytes]:
        try:
            st, c = self.Sigma[w]
        except Exception as ex:
            if str(ex) != "key not found":
                raise ex
            st = os.urandom(16)
            c = 0

        st_concat_c_plus_1 = st + uint_to_bytes(c + 1)
        u = H1(st_concat_c_plus_1)
        e = xor_bytes(ind + op.to_bytes(), itertools.cycle(H2(st_concat_c_plus_1)))

        self.Sigma[w] = (st, c + 1)

        return u, e

    def search_client_part(self, w: bytes) -> Tuple[bytes, bytes | None, int] | None:
        try:
            st, c = self.Sigma[w]
        except Exception as ex:
            if str(ex) != "key not found":
                raise ex
            return None

        t_w = F(self.k_s, h(w))

        if c != 0:
            k_w = st
            st = os.urandom(16)

            self.Sigma[w] = (st, 0)
        else:
            k_w = None

        return t_w, k_w, c
