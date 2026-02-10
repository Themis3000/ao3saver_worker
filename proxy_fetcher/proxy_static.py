from typing import Dict
import os
from .proxy_base import ProxyBase


class ProxyStatic(ProxyBase):
    def __init__(self):
        if "PROXYADDRESS" not in os.environ:
            raise Exception("Could not init ProxyStatic, no proxy address found!")
        proxy_str = os.environ["PROXYADDRESS"]
        self.proxy = {"http": proxy_str, "https": proxy_str}

    def get_proxy(self) -> Dict[str, str]:
        return self.proxy
