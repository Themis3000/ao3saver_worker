import os
from .proxy_base import ProxyBase, ProxyInfo


class ProxyStatic(ProxyBase):
    def __init__(self):
        if "PROXYADDRESS" not in os.environ:
            raise Exception("Could not init ProxyStatic, no proxy address found!")
        proxy_str = os.environ["PROXYADDRESS"]
        self.proxy = ProxyInfo(dict={"http": proxy_str, "https": proxy_str})

    def get_proxy(self) -> ProxyInfo:
        return self.proxy
