import asyncio
from typing import TYPE_CHECKING, Literal

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import JSON, Column, Field, SQLModel, create_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from .log import log

if TYPE_CHECKING:
    from collections.abc import Iterable


class Bangumi_ani_getter:
    LIMITE = 100

    def __init__(
        self,
        *,
        database_filename: str,
        proxy: httpx._types.ProxyTypes | None = None,
        timeout: httpx._types.TimeoutTypes = 10,
    ) -> None:
        self.req_queue: asyncio.Queue = asyncio.Queue()
        self.client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,  # 允许重定向
            proxy=proxy,
            timeout=timeout,
        )
        if not database_filename.endswith(".db"):
            database_filename += ".db"
        self.sync_engine = create_engine(f"sqlite:///{database_filename}")
        self.async_engine = create_async_engine(
            f"sqlite+aiosqlite:///{database_filename}"
        )
        self.__class__.Bangumi_ani_data.metadata.create_all(self.sync_engine)

    def __del__(self) -> None:
        asyncio.run(self.client.aclose())

    class Res_content_data_infobox_别名_value_item(BaseModel):
        model_config = ConfigDict(extra="allow")

        v: str

    class Res_content_data_infobox(BaseModel):
        model_config = ConfigDict(extra="allow")

        key: str
        value: str | list[Bangumi_ani_getter.Res_content_data_infobox_别名_value_item]

    class Res_content_data(BaseModel):
        model_config = ConfigDict(extra="allow")

        id: int
        name: str
        name_cn: str
        infobox: list[Bangumi_ani_getter.Res_content_data_infobox]

    class Res_content(BaseModel):
        data: list[Bangumi_ani_getter.Res_content_data]
        total: int
        limit: int
        offset: int

    async def auto_req(self):
        """自动循环爬取"""

        async def _req(offset: int, /) -> Bangumi_ani_getter.Res_content | None:
            try:
                log.debug("开始请求")
                response = await self.client.get(
                    "https://api.bgm.tv/v0/subjects",
                    params={
                        "type": 2,
                        "sort": "date",
                        "limit": self.__class__.LIMITE,
                        "offset": offset,
                    },
                )
                log.debug("请求头: {}", response.request.headers)
                response.raise_for_status()
                log.debug(
                    "响应头: {} {} {}",
                    response.http_version,
                    response.status_code,
                    response.headers,
                )

                return self.__class__.Res_content(**response.json())

            except httpx.HTTPStatusError as e:
                log.error("状态码错误: {}", e.response.status_code)
            except httpx.ConnectError as e:
                log.error("连接失败: {}", e)
            except httpx.TimeoutException:
                log.warning("请求超时")
            except ValidationError as e:
                log.error("类型错误: {}", e)
                raise

        cycle_num: int = 1
        sleep_time: Literal[1, 10] = 1
        total: int | None = None
        offset: int = 0
        try:
            while True:
                res = await _req(offset)
                log.debug(
                    "请求结果: {}",
                    res
                    if res is None
                    else {**res.model_dump(), "data": f"data_len = {len(res.data)}"},
                )
                if res is None:
                    continue

                total = res.total
                offset += self.__class__.LIMITE
                if offset > total:
                    offset = 0
                    sleep_time = 10  # 循环完一遍，进入慢速循环
                    cycle_num += 1
                    log.info("进入第 {} 次循环", cycle_num)

                log.debug("写入数据库")
                ani_data_list: list[Bangumi_ani_getter.Bangumi_ani_data] = []
                for data in res.data:
                    别名_list: list[str] = []
                    for info in data.infobox:
                        if info.key == "别名":
                            if isinstance(info.value, str):
                                别名_list = [info.value]
                            else:
                                别名_list = [v.v for v in info.value]
                            break
                    ani_data_list.append(
                        self.__class__.Bangumi_ani_data(
                            id=data.id,
                            name=data.name,
                            name_cn=data.name_cn,
                            name_alias=别名_list,
                        )
                    )

                await self.save_data(ani_data_list)

                log.debug("请求等待 {} sec", sleep_time)
                await asyncio.sleep(sleep_time)

        except ValidationError as e:
            raise ValidationError("类型检查不通过，终止循环请求") from e

    class Bangumi_ani_data(SQLModel, table=True):
        id: int = Field(primary_key=True)
        name: str
        name_cn: str
        name_alias: list[str] = Field(sa_column=Column(JSON))

    async def save_data(self, datas: Iterable[Bangumi_ani_data]):
        async with AsyncSession(self.async_engine) as session:
            for data in datas:
                await session.merge(data)
            await session.commit()
