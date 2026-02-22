import asyncio
import urllib.request

from .getter import Bangumi_ani_getter
from .log import log


async def run_example():
    proxies: dict[str, str] = urllib.request.getproxies()
    proxy_url = proxies.get("http")
    if proxy_url is not None:
        proxy_url = proxy_url.split("//", maxsplit=1)[-1].strip()
        proxy_url = f"socks5://{proxy_url}"

    bangumi_ani_getter = Bangumi_ani_getter(
        database_filename="OIDBT_SQLite",
        proxy=proxy_url,
    )

    await asyncio.gather(bangumi_ani_getter.auto_req())


if __name__ == "__main__":
    log.print_level = log.LogLevel.debug
    asyncio.run(run_example())
