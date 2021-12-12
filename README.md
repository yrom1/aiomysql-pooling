# aiomysql-pooling

```py
# asyncio.run(no_pool_main())  # 34.54226851463318 seconds
asyncio.run(pool_main())  # 5.571674585342407 seconds
```
```py
>>> 34.5 / 5.57
6.193895870736086
```
6X improvement making 10,000 MySQL `SELECT 1` queries with connection pooling.
