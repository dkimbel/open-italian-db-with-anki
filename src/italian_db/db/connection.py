"""Database connection management using SQLAlchemy."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Engine, create_engine, event
from sqlalchemy.pool import ConnectionPoolEntry

DEFAULT_DB_PATH = Path("italian.db")

_engine_cache: dict[Path, Engine] = {}


def _set_sqlite_pragma(dbapi_connection: Any, _connection_record: ConnectionPoolEntry) -> None:
    """Enable foreign keys for SQLite connections."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def get_engine(db_path: Path | str = DEFAULT_DB_PATH) -> Engine:
    """Get or create a SQLAlchemy engine for the given database path.

    Engines are cached by path to avoid creating multiple engines for the same database.
    Enables foreign key enforcement for SQLite.
    """
    db_path = Path(db_path)

    if db_path not in _engine_cache:
        engine = create_engine(f"sqlite:///{db_path}", echo=False)
        event.listen(engine, "connect", _set_sqlite_pragma)
        _engine_cache[db_path] = engine

    return _engine_cache[db_path]


@contextmanager
def get_connection(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> Generator[Connection]:
    """Context manager for database connections.

    Automatically commits on success, rolls back on exception.

    Example:
        with get_connection() as conn:
            result = conn.execute(select(lemmas).where(lemmas.c.lemma == "parlare"))
            for row in result:
                print(row.lemma_stressed)
    """
    engine = get_engine(db_path)
    with engine.connect() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
