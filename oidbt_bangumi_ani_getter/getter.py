import asyncio
from typing import TYPE_CHECKING, ClassVar, Literal, NoReturn

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import (
    JSON,
    Column,
    Field,
    SQLModel,
    and_,
    create_engine,
    delete,
    func,
    select,
)
from sqlmodel.ext.asyncio.session import AsyncSession

from .log import log

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


class Bangumi_ani_getter:
    DATABASE_LOCK: ClassVar = asyncio.Lock()
    LIMITE: ClassVar = 100

    def __init__(
        self,
        *,
        database_filename: str,
        proxy: httpx._types.ProxyTypes | None = None,
        timeout: httpx._types.TimeoutTypes = 10,
    ) -> None:
        self.client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,  # 允许重定向
            proxy=proxy,
            timeout=timeout,
        )
        """HTTP Client"""
        if not database_filename.endswith(".db"):
            database_filename += ".db"
        self.sync_engine = create_engine(f"sqlite:///{database_filename}")
        """同步 database engine"""
        self.async_engine = create_async_engine(
            f"sqlite+aiosqlite:///{database_filename}"
        )
        """异步 database engine"""
        self.Bangumi_ani_data.metadata.create_all(self.sync_engine)

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

    async def auto_req(self) -> NoReturn:
        """自动循环爬取"""

        async def _req(offset: int, /) -> Bangumi_ani_getter.Res_content | None:
            try:
                log.debug("{} 开始请求", self.__class__.__name__)
                response = await self.client.get(
                    "https://api.bgm.tv/v0/subjects",
                    params={
                        "type": 2,
                        "sort": "date",
                        "limit": self.LIMITE,
                        "offset": offset,
                    },
                )
                log.debug(
                    "{} 请求头: {}",
                    self.__class__.__name__,
                    response.request.headers,
                    print_level=log.LogLevel._detail,
                )
                response.raise_for_status()
                log.debug(
                    "{} 响应头: {} {} {}",
                    self.__class__.__name__,
                    response.http_version,
                    response.status_code,
                    response.headers,
                    print_level=log.LogLevel._detail,
                )

                return self.Res_content(**response.json())

            except httpx.HTTPStatusError as e:
                log.error(
                    "{} 状态码错误: {}", self.__class__.__name__, e.response.status_code
                )
            except httpx.ConnectError as e:
                log.error("{} 连接失败: {}", self.__class__.__name__, e)
            except httpx.TimeoutException:
                log.warning("{} 请求超时", self.__class__.__name__)
            except ValidationError as e:
                log.error("{} 类型错误: {}", self.__class__.__name__, e)
                raise

        cycle_num: int = 1
        sleep_time: Literal[1, 10] = 1
        total: int | None = None
        offset: int = 0
        try:
            while True:
                res = await _req(offset)
                log.debug(
                    "{} 请求结果: {}",
                    self.__class__.__name__,
                    res
                    if res is None
                    else {**res.model_dump(), "data": f"data_len = {len(res.data)}"},
                )
                if res is None:
                    continue

                total = res.total
                offset += self.LIMITE
                if offset > total:
                    offset = 0
                    sleep_time = 10  # 循环完一遍，进入慢速循环
                    cycle_num += 1
                    log.info("{} 进入第 {} 次循环", self.__class__.__name__, cycle_num)

                log.debug("{} 写入数据库", self.__class__.__name__)
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
                        self.Bangumi_ani_data(
                            id=data.id,
                            name=data.name,
                            name_cn=data.name_cn,
                            name_alias=别名_list,
                        )
                    )

                await self.save_data(ani_data_list)

                log.debug("{} 请求等待 {} sec", self.__class__.__name__, sleep_time)
                await asyncio.sleep(sleep_time)

        except ValidationError as e:
            raise TypeError("类型检查不通过，终止循环请求") from e

    class Bangumi_ani_data(SQLModel, table=True):
        id: int = Field(description="Bangumi 条目 ID", primary_key=True)
        name: str = Field(description="条目名")
        name_cn: str = Field(description="条目中文名")
        name_alias: list[str] = Field(
            description="条目别名列表", sa_column=Column(JSON)
        )

    async def save_data(
        self,
        datas: Iterable[Bangumi_ani_data],
        *,
        refresh: bool = True,
    ) -> None:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            for data in datas:
                if refresh:
                    stmt = delete(self.Bangumi_ani_data).where(
                        and_(self.Bangumi_ani_data.id == data.id)
                    )
                    await session.exec(stmt)
                    session.add(data)
                else:
                    await session.merge(data)
            await session.commit()

    async def get_data(self) -> Sequence[Bangumi_ani_data]:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            return (await session.exec(select(self.Bangumi_ani_data))).all()

    async def get_data_len(self) -> int:
        async with (
            self.DATABASE_LOCK,
            AsyncSession(self.async_engine) as session,
        ):
            statement = select(func.count()).select_from(self.Bangumi_ani_data)
            return (await session.exec(statement)).one()
