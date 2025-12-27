from sqlalchemy.ext.asyncio import create_async_engine


DB_URL = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule"
engine = create_async_engine(
    url=DB_URL,
    pool_size=20,
    max_overflow=40,
    echo=True
)