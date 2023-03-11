from abc import abstractmethod
import ctypes
from typing import TypeVar, Generic, List

T = TypeVar('T')

class Repository(Generic[T]):
    @abstractmethod
    def find(self, condition) -> List[T]:
        pass

    @abstractmethod
    def find_first(self, condition) -> T:
        pass

    @abstractmethod
    def insert(self, model: T) -> T:
        pass

    @abstractmethod
    def update(self, key, model: T) -> T:
        pass

    @abstractmethod
    def delete(self, condition):
        pass

    @abstractmethod
    def any(self, model: T) -> bool:
        pass