from abc import ABCMeta, abstractmethod
from typing import Any, List

Key = bytes | str | int | float | bool


class Map(metaclass=ABCMeta):
    @abstractmethod
    def __setitem__(self, key: Key, item: Any):
        pass

    @abstractmethod
    def __getitem__(self, key: Key) -> Any:
        pass

    @abstractmethod
    def __delitem__(self, key: Key):
        pass

    @abstractmethod
    def __contains__(self, key: Key) -> bool:
        pass


class Storage(metaclass=ABCMeta):
    @abstractmethod
    def get_namespace(self, namespace: str):
        pass

    @abstractmethod
    def namespaces(self) -> List[str]:
        pass

    @abstractmethod
    def names(self) -> List[str]:
        pass

    @abstractmethod
    def create_map(self, name: str) -> Map:
        pass

    @abstractmethod
    def get_map(self, name: str) -> Map:
        pass

    @abstractmethod
    def drop_map(self, name: str) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
