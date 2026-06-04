import asyncio

import asyncpg


async def test():
    conn = await asyncpg.connect("postgresql://hermes:hermes_secret@127.0.0.1:5432/hermes_db?sslmode=disable")
    print("OK:", await conn.fetchval("SELECT 1"))
    await conn.close()
asyncio.run(test())
