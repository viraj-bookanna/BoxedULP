import os
import logging
from typing import Sequence
from urllib.parse import urlparse
from sqlalchemy import create_engine, Column, Integer, String, insert
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv(override=True)
engine = create_engine(os.getenv("SQL_CONNECTION_STRING"))
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class Credential(Base):
    """SQLAlchemy model for a stored credential row."""

    __tablename__ = "credentials"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), nullable=False)
    password = Column(String(100), nullable=False)
    login_url = Column(String(2048), nullable=False)
    domain = Column(String(253), nullable=False)

    def __repr__(self):
        return f"<Credential(username='{self.username}', login_url='{self.login_url}')>"


class DatabaseSQL:
    """Thin wrapper around the credentials table for bulk inserts."""

    def __init__(self) -> None:
        Base.metadata.create_all(engine)

    @staticmethod
    def insert_combos(combolist: Sequence[tuple], chunk_size: int = 1000) -> int:
        """Bulk-insert credentials using SQLAlchemy Core.

        Args:
            combolist: Sequence of (username, password, login_url) tuples.
            chunk_size: Number of rows per INSERT batch.

        Returns:
            Total number of rows inserted.
        """
        bulk_data = []
        for u, p, url in combolist:
            try:
                bulk_data.append(
                    {
                        "username": u,
                        "password": p,
                        "login_url": url,
                        "domain": urlparse(url).netloc,
                    }
                )
            except Exception:  # pylint: disable=broad-exception-caught
                logging.debug("Skipping malformed combo entry: %s", (u, url), exc_info=True)
        total_inserted = 0
        logging.info("Inserting %d credentials in chunks of %d", len(bulk_data), chunk_size)
        dialect = engine.dialect.name
        for i in range(0, len(bulk_data), chunk_size):
            chunk = bulk_data[i : i + chunk_size]
            try:
                with engine.begin() as conn:
                    stmt = insert(Credential)
                    if dialect == "sqlite":
                        stmt = stmt.prefix_with("OR IGNORE")
                    elif dialect == "mysql":
                        stmt = stmt.prefix_with("IGNORE")
                    conn.execute(stmt, chunk)
                    total_inserted += len(chunk)
            except Exception:  # pylint: disable=broad-exception-caught
                logging.warning("Chunk insert failed at offset %d", i, exc_info=True)
        logging.info("Inserted %d credentials total", total_inserted)
        return total_inserted
