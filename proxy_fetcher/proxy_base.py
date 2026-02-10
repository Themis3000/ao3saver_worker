from abc import ABC, abstractmethod
from typing import Dict


class ProxyBase(ABC):
    @abstractmethod
    def get_proxy(self) -> Dict[str, str]:
        pass
