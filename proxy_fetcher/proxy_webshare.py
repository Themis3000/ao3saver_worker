import requests
import os
from .proxy_base import ProxyBase, ProxyInfoExtended
from typing import List
import random


class ProxyWebshare(ProxyBase):
    def __init__(self):
        self.api_key = os.environ.get("WEBSHARE_API_KEY")
        if self.api_key is None:
            raise Exception("Could not init ProxyWebshare! No API key found")
        self.proxies: List[ProxyInfoExtended] = []
        self.refresh_proxies()

    def refresh_proxies(self):
        self.proxies.clear()
        plans = self.get_plans()
        for plan in plans:
            if plan["status"] != "active":
                continue
            self.fetch_proxies(plan["id"])

    def fetch_proxies(self, plan_id: str):
        response = requests.get(
            f"https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=10000&plan_id={plan_id}",
            headers={"Authorization": self.api_key}
        )
        if not response.ok:
            raise Exception(f"Non-okay response from webshare.io.\n"
                            f"HTTP code: {response.status_code}.\n"
                            f"Message: {response.text}")
        response_json = response.json()

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

    def get_plans(self):
        response = requests.get(
            "https://proxy.webshare.io/api/v2/subscription/plan/",
            headers={"Authorization": self.api_key}
        )
        if not response.ok:
            raise Exception(f"Non-okay response from webshare.io.\n"
                            f"HTTP code: {response.status_code}.\n"
                            f"Message: {response.text}")

        return response.json()["results"]

    def get_proxy(self) -> ProxyInfoExtended:
        return random.choice(self.proxies)
