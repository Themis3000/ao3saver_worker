from typing import Dict
from .proxy_base import ProxyBase


class ProxyNone(ProxyBase):
    def get_proxy(self) -> Dict[str, str]:
        return {}
