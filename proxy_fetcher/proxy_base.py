from abc import ABC, abstractmethod
from typing import Dict
from dataclasses import dataclass


@dataclass
class ProxyInfo:
    dict: Dict[str, str]


@dataclass
class ProxyInfoExtended(ProxyInfo):
    address: str


class ProxyBase(ABC):
    @abstractmethod
    def get_proxy(self) -> ProxyInfo:
        pass
