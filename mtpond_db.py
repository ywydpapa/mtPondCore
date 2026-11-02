"""공통 비동기 DB 엔진 및 세션 설정."""

import os

import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


dotenv.load_dotenv()

DATABASE_URL = os.getenv("dburl")
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
