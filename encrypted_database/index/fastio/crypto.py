from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

H1_HMAC_KEY = b"1"


def H1(msg: bytes) -> bytes:
    h = hmac.HMAC(H1_HMAC_KEY, hashes.SHA256())
    h.update(msg)

    return h.finalize()


H2_HMAC_KEY = b"2"


def H2(msg: bytes) -> bytes:
    h = hmac.HMAC(H2_HMAC_KEY, hashes.SHA256())
    h.update(msg)

    return h.finalize()


def h(msg: bytes) -> bytes:
    hash = hashes.Hash(hashes.SHA256())
    hash.update(msg)

    return hash.finalize()


def F(key: bytes, data: bytes) -> bytes:
    cipher = Cipher(algorithms.AES128(key), modes.ECB())
    encryptor = cipher.encryptor()

    return encryptor.update(data) + encryptor.finalize()
