import asyncio
import time
from configparser import ConfigParser
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
    Union,
)

import aiomysql

DBParameters = Union[Tuple[Any, ...], Dict[Any, Any]]

POOL: Optional[aiomysql.pool.Pool] = None
N = 10000


class DBExecutor(Protocol):
    def __call__(
        self, command: str, data: DBParameters = tuple(), /
    ) -> Awaitable[List[Tuple[Any, ...]]]:
        ...


def config(filename="config.ini", section="mysql") -> Dict[str, str]:
    parser = ConfigParser()
    parser.read(filename)
    config_dict: Dict[str, str] = {}
    if parser.has_section(section):
        for item in parser.items(section):
            key, value = item
            config_dict[key] = value
    else:
        raise Exception(f"Section {section} not found in the {filename} file")

    return config_dict


async def make_pool() -> aiomysql.pool.Pool:
    global POOL
    if POOL is None:
        POOL = await aiomysql.create_pool(maxsize=10, **config())
    return POOL


async def kill_pool() -> None:
    global POOL
    if POOL is not None:
        POOL.close()
        await POOL.wait_closed()


async def pool_execute(
    command: str,
    data: DBParameters = tuple(),
    /,
) -> List[Tuple[Any, ...]]:
    try:
        pool = POOL
        if pool is None:
            pool = await aiomysql.create_pool(**config())
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(command, data)
                result = await cursor.fetchall()
                await conn.commit()

    except Exception as e:
        if not None:
            print(f"{command=}")
        raise e

    return result


async def no_pool_execute(
    command: str,
    data: DBParameters = tuple(),
    /,
) -> List[Tuple[Any, ...]]:
    try:
        conn = await aiomysql.connect(**config())
        cursor = await conn.cursor()
        await cursor.execute(command, data)
        result = await cursor.fetchall()
        await conn.commit()

    except Exception as e:
        if not None:
            print(f"{command=}")
        raise e

    finally:
        await cursor.close()
        conn.close()

    return result


async def dummy(executor: DBExecutor) -> None:
    result = await executor("select 1")
    # print(result[0][0])


async def no_pool_main() -> None:
    start = time.time()
    executor = no_pool_execute
    for i in range(N):
        await dummy(executor)
        await dummy(executor)
    print(time.time() - start, "seconds")


async def pool_main() -> None:
    start = time.time()
    executor = pool_execute
    await make_pool()
    for i in range(N):
        await dummy(executor)
        await dummy(executor)
    await kill_pool()
    print(time.time() - start, "seconds")


# asyncio.run(no_pool_main())  # 34.54226851463318 seconds
asyncio.run(pool_main())  # 5.571674585342407 seconds
