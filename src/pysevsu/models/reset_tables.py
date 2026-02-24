import asyncio

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .tables import Base


async def drop_all_tables(engine: AsyncEngine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


if __name__ == "__main__":
    engine = create_async_engine(url="postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule")
    asyncio.run(drop_all_tables(engine))
