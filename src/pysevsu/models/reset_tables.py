import asyncio

from sqlalchemy.ext.asyncio import AsyncEngine

from .tables import Base


async def drop_all_tables(engine: AsyncEngine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


if __name__ == "__main__":
    from .session import engine

    asyncio.run(drop_all_tables(engine))
