import requests
import os
from .proxy_base import ProxyBase, ProxyInfoExtended
from .utils import forever_iter
from typing import List


class ProxyWebshare(ProxyBase):
    def __init__(self):
        self.api_key = os.environ.get("WEBSHARE_API_KEY")
        if self.api_key is None:
            raise Exception("Could not init ProxyWebshare! No API key found")
        self.proxies: List[ProxyInfoExtended] = []
        self.proxy_iter = forever_iter(self.proxies)
        self.refresh_proxies()

    def refresh_proxies(self):
        response = requests.get(
            "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=10000",
            headers={"Authorization": self.api_key}
        )
        if not response.ok:
            raise Exception(f"Non-okay response from webshare.io.\n"
                            f"HTTP code: {response.status_code}.\n"
                            f"Message: {response.text}")
        response_json = response.json()

        self.proxies.clear()
        for proxy_info in response_json["results"]:
            if not proxy_info['valid']:
                continue
            username = proxy_info['username']
            password = proxy_info['password']
            address = proxy_info['proxy_address']
            port = proxy_info['port']
            proxy_str = f"http://{username}:{password}@{address}:{port}"
            proxy_obj = ProxyInfoExtended(dict={"http": proxy_str, "https": proxy_str}, address=address)
            self.proxies.append(proxy_obj)
        self.proxy_iter = forever_iter(self.proxies)

    def get_proxy(self) -> ProxyInfoExtended:
        return next(self.proxy_iter)
