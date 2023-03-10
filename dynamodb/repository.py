from abc import abstractmethod
from typing import TypeVar, Generic, List

T = TypeVar('T')

class Repository(Generic[T]):
    @abstractmethod
    def get(self, condition) -> T:
        pass

    @abstractmethod
    def get_first(self, condition) -> T:
        pass

    @abstractmethod
    def insert(self, model: T):
        pass