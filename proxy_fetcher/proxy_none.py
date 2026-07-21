from .proxy_base import ProxyBase, ProxyInfo


class ProxyNone(ProxyBase):
    def get_proxy(self) -> ProxyInfo:
        return ProxyInfo(dict={})
