from enum import Enum


class Opterator(Enum):
    ADD = b"0"
    DEL = b"1"

    def to_bytes(self) -> bytes:
        return self.value

    @classmethod
    def from_bytes(cls, src: bytes):
        return cls(src)
