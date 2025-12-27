from asyncio import run
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from .tables import Base
from .engine import engine

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def _create():
    async with engine.begin() as conn: 
        await conn.run_sync(Base.metadata.create_all)

if __name__ == "__main__":
    run(_create())
