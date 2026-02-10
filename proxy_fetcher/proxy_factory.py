import os
from .proxy_base import ProxyBase
from .proxy_static import ProxyStatic
from .proxy_webshare import ProxyWebshare
from .proxy_none import ProxyNone


def proxy_fetcher_factory() -> ProxyBase:
    proxy_type = os.environ.get("PROXY_TYPE", "no_selection").lower()
    if proxy_type == "no_selection":
        raise Exception("Could not build proxy, PROXY_TYPE was not defined")
    if proxy_type == "none":
        return ProxyNone()
    if proxy_type == "static":
        return ProxyStatic()
    if proxy_type == "webshare":
        return ProxyWebshare()
