from requests_html import AsyncHTMLSession
from requests.exceptions import RequestException
import asyncio
import os
from dotenv import load_dotenv
load_dotenv()

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
IP_CHECKERS = [
    "https://checkip.amazonaws.com",
    "https://api.ipify.org",
    "https://icanhazip.com"
]


class ProxySetupError(Exception):
    """Raised when the session and proxy cannot be initialized successfully."""
    pass

class NordVPNSession:

    def __init__(self, max_retries = 10):
        self.addresses = [
            "amsterdam.nl.socks.nordhold.net", "atlanta.us.socks.nordhold.net",
            "dallas.us.socks.nordhold.net", "los-angeles.us.socks.nordhold.net",
            "nl.socks.nordhold.net", "se.socks.nordhold.net",
            "stockholm.se.socks.nordhold.net", "us.socks.nordhold.net",
            "new-york.us.socks.nordhold.net", "san-francisco.us.socks.nordhold.net",
            "chicago.us.socks.nordhold.net", "phoenix.us.socks.nordhold.net"
        ]
        self.proxy_index: int = 0
        self.nord_user = os.getenv("NORD_USER")
        self.nord_pass = os.getenv("NORD_PASS")
        if not self.nord_user or not self.nord_pass:
            raise ValueError("Environment variables NORD_USER and NORD_PASS must be set.")
        self.session = AsyncHTMLSession()
        self.naked_ip: str = None
        self.max_retries = max_retries

    def __getattr__(self, name):
        """
        Delegates any method/attribute not found in NordVPNSession
        to the internal self.session (AsyncHTMLSession).
        """
        return getattr(self.session, name)


    async def create_and_configure_session(self):
        if self.session:
            await self.session.close()
        self.session = AsyncHTMLSession()
        self.session.headers.update({'user-agent': USER_AGENT})
        proxy_address = self.addresses[self.proxy_index]
        proxy_url = f"socks5://{self.nord_user}:{self.nord_pass}@{proxy_address}:1080"
        self.session.proxies.update({"http": proxy_url, "https": proxy_url})

    async def initialize(self):
        self.naked_ip = await self.get_ip(use_proxy=False)
        if not self.naked_ip:
            print("Can't verify naked IP. Aborting.")
            raise ProxySetupError("Could not verify naked IP.")
        for attempt in range(self.max_retries):
            await self.create_and_configure_session()
            proxy_ip = await self.get_ip(use_proxy=True)
            if proxy_ip and self.naked_ip != proxy_ip:
                print(f"Proxy IP is different from naked IP. \\ Proxy IP: {proxy_ip} \\ Naked IP: {self.naked_ip} \\ That's fine, continuing process.")
                return
            self.proxy_index = (self.proxy_index + 1) % len(self.addresses) # Rotating proxy
            print(f"Proxy check failed or IP is naked. Rotating to index {self.proxy_index}...")

        raise ProxySetupError(f"Failed to establish a working proxy after {self.max_retries} attempts.")

    async def get_ip(self, use_proxy: bool) -> str:
        active_session = self.session if use_proxy else AsyncHTMLSession()
        try:
            for url in IP_CHECKERS:
                try:
                    response = await active_session.get(url, timeout=10)
                    if response.status_code == 200:
                        return response.text.strip()
                except Exception:
                    continue
        finally:
            if not use_proxy:
                await active_session.close()
        return None

    async def get(self, url, **kwargs):
        """
        Performs a GET request with automatic retry and proxy rotation on failure.
        """
        for attempt in range(self.max_retries):
            try:
                response = await self.session.get(url, **kwargs)
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    print(f"Error 404 recieved on {url}.")
                    print(f"Response reason: {response.reason}")
                    if response.text:
                        print(f"Reason text: {response.text}")
                    else:
                        print("Reason text: (No additional details provided by server)")
                    return None
                else:
                    print(f"Request failed with status {response.status_code}. Rotating proxy and retrying ({attempt + 1}/{self.max_retries})...")
            except Exception as e:
                print(f"Request failed with error: {e}. Rotating proxy and retrying ({attempt + 1}/{self.max_retries})...")
            
            # Rotate proxy
            self.proxy_index = (self.proxy_index + 1) % len(self.addresses)
            await self.create_and_configure_session()
            
        raise Exception(f"Failed to fetch {url} after {self.max_retries} attempts.")
