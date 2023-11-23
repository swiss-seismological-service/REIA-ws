
from sqlalchemy import Select, create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from config import get_settings

engine = create_engine(
    get_settings().SQLALCHEMY_DATABASE_URL, pool_size=10, max_overflow=10)
SessionLocal = sessionmaker(autocommit=False,
                            autoflush=False,
                            bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def paginate(
        session: Session, query: Select, limit: int, offset: int) -> dict:
    return {
        'count': session.scalar(select(func.count())
                                .select_from(query.subquery())),
        'items': [todo for todo in session.scalars(query.limit(limit)
                                                   .offset(offset))]
    }
