import contextlib
from typing import Annotated, Any, AsyncIterator

import pandas as pd
from fastapi import Depends
from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import (AsyncConnection, AsyncSession,
                                    async_sessionmaker, create_async_engine)
from sqlalchemy.orm import declarative_base

from config import get_settings

Base = declarative_base()


class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}):
        self._engine = create_async_engine(
            host, pool_size=10, max_overflow=5, **engine_kwargs)
        self._sessionmaker = async_sessionmaker(
            autocommit=False, autoflush=False, bind=self._engine)

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


sessionmanager = DatabaseSessionManager(
    get_settings().SQLALCHEMY_DATABASE_URL, {"echo": False})


async def get_db():
    async with sessionmanager.session() as session:
        yield session

DBSessionDep = Annotated[AsyncSession, Depends(get_db)]


async def paginate(
        session: AsyncSession, query: Select, limit: int, offset: int) -> dict:
    return {
        'count': await session.scalar(select(func.count())
                                      .select_from(query.subquery())),
        'items': [todo for todo in await session.scalars(query.limit(limit)
                                                         .offset(offset))]
    }


async def pandas_read_sql(stmt):
    """
    wrapper around pandas read_sql to use sqlalchemy engine
    and correctly close and dispose of the connections
    afterwards.
    """
    def read_sql_query(con, s):
        return pd.read_sql_query(s, con)

    async with sessionmanager.connect() as con:
        df = await con.run_sync(read_sql_query, stmt)

    return df
