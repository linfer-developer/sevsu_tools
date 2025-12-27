import asyncio
from .tables import Base
from sqlalchemy.ext.asyncio import AsyncEngine

async def drop_all_tables(engine: AsyncEngine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

if __name__ == "__main__":
    from .engine import engine
    asyncio.run(drop_all_tables(engine))