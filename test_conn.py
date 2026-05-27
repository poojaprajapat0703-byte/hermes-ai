import asyncio

import asyncpg


async def t():
    conn = await asyncpg.connect(
        host="localhost",
        port=5432,
        user="hermes",
        password="hermes_secret",
        database="hermes_db"
    )
    print(await conn.fetchval("SELECT current_user"))
    await conn.close()

asyncio.run(t())
