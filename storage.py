import copy
from contextlib import contextmanager
import json
import logging
import os
import pickle
import tempfile
from pathlib import Path

import config
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

_db_engine = None
_session_factory = None
CURRENT_SCHEMA_VERSION = 1

APP_CONFIG_DEFAULTS = {
    "sidebarTabs": {
        "raceDatabase": True,
        "pedigreeLists": True,
        "autoPickStrategy": True,
        "weekendWatchlist": True,
    },
    "ui": {
        "riskSlider": 50,
        "betSafetyIndicator": True,
        "voteSortingTop": True,
        "autoFetchPastResults": True,
        "autoLockPastVotes": False,
        "showConsole": True,
    },
}

DB_METADATA = MetaData()

schema_migrations_table = Table(
    "schema_migrations",
    DB_METADATA,
    Column("version", Integer, primary_key=True),
    Column("applied_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

app_config_table = Table(
    "app_config",
    DB_METADATA,
    Column("config_key", String(128), primary_key=True),
    Column("config_value", JSON, nullable=False),
    Column("updated_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

horses_table = Table(
    "horses",
    DB_METADATA,
    Column("horse_id", String(32), primary_key=True),
    Column("horse_name", String(255), nullable=False, server_default=text("''")),
    Column("sire", String(255), nullable=False, server_default=text("''")),
    Column("dam", String(255), nullable=False, server_default=text("''")),
    Column("bms", String(255), nullable=False, server_default=text("''")),
    Column("pedigree", JSON, nullable=False, server_default=text("'{}'")),
    Column("raw_payload", JSON, nullable=False, server_default=text("'{}'")),
    Column("updated_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

tracked_horses_table = Table(
    "tracked_horses",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("horse_id", String(32), nullable=False, unique=True),
    Column("added_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

watchlist_horses_table = Table(
    "watchlist_horses",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("horse_id", String(32), nullable=False, unique=True),
    Column("added_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

races_table = Table(
    "races",
    DB_METADATA,
    Column("race_id", String(32), primary_key=True),
    Column("race_date", String(16), nullable=False, server_default=text("''")),
    Column("kaisai_id", String(32), nullable=False, server_default=text("''")),
    Column("track", String(128), nullable=False, server_default=text("''")),
    Column("race_name", String(255), nullable=False, server_default=text("''")),
    Column("race_number", Integer, nullable=False, server_default=text("0")),
    Column("sort_time", String(32), nullable=False, server_default=text("''")),
    Column("distance", Integer, nullable=False, server_default=text("0")),
    Column("surface", String(32), nullable=False, server_default=text("''")),
    Column("grade", String(32), nullable=False, server_default=text("''")),
    Column("raw_payload", JSON, nullable=False, server_default=text("'{}'")),
    Column("updated_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

race_entries_table = Table(
    "race_entries",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("race_id", String(32), ForeignKey("races.race_id", ondelete="CASCADE"), nullable=False),
    Column("horse_id", String(32), nullable=False, server_default=text("''")),
    Column("horse_name", String(255), nullable=False, server_default=text("''")),
    Column("frame_number", Integer, nullable=False, server_default=text("0")),
    Column("horse_number", Integer, nullable=False, server_default=text("0")),
    Column("jockey", String(255), nullable=False, server_default=text("''")),
    Column("odds", Float, nullable=False, server_default=text("0")),
    Column("finish_position", Integer, nullable=False, server_default=text("0")),
    Column("entry_score", Float, nullable=False, server_default=text("0")),
    Column("mark_symbol", String(8), nullable=False, server_default=text("''")),
    Column("raw_payload", JSON, nullable=False, server_default=text("'{}'")),
    Column("updated_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    UniqueConstraint("race_id", "horse_number", name="uq_race_entries_race_horse_number"),
)

race_marks_table = Table(
    "race_marks",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("race_id", String(32), nullable=False),
    Column("horse_key", String(64), nullable=False),
    Column("mark_symbol", String(8), nullable=False, server_default=text("''")),
    Column("updated_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    UniqueConstraint("race_id", "horse_key", name="uq_race_marks_race_horse_key"),
)

race_metadata_table = Table(
    "race_metadata",
    DB_METADATA,
    Column("race_id", String(32), primary_key=True),
    Column("saved_at", String(64), nullable=False, server_default=text("''")),
    Column("updated_at", String(64), nullable=False, server_default=text("''")),
    Column("mark_source", String(64), nullable=False, server_default=text("''")),
    Column("strategy_snapshot", JSON, nullable=False, server_default=text("'{}'")),
    Column("manual_adjustments", Integer, nullable=False, server_default=text("0")),
    Column("lock_state_at_save", Boolean, nullable=True),
    Column("active_symbols", JSON, nullable=False, server_default=text("'[]'")),
)

orepro_profiles_table = Table(
    "orepro_profiles",
    DB_METADATA,
    Column("profile_id", String(32), primary_key=True),
    Column("username", String(255), nullable=False, server_default=text("''")),
    Column("created_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

orepro_sessions_table = Table(
    "orepro_sessions",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("profile_id", String(32), ForeignKey("orepro_profiles.profile_id", ondelete="SET NULL"), nullable=True),
    Column("nkauth", Text, nullable=False, server_default=text("''")),
    Column("updated_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

orepro_daily_results_table = Table(
    "orepro_daily_results",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("date_key", String(16), nullable=False),
    Column("profile_id", String(32), ForeignKey("orepro_profiles.profile_id", ondelete="CASCADE"), nullable=False),
    Column("username", String(255), nullable=False, server_default=text("''")),
    Column("kaisai_id", String(32), nullable=False, server_default=text("''")),
    Column("resolved_kaisai_ids", JSON, nullable=False, server_default=text("'[]'")),
    Column("is_partial", Boolean, nullable=False, server_default=text("0")),
    Column("races", Integer, nullable=False, server_default=text("0")),
    Column("purchase", Integer, nullable=False, server_default=text("0")),
    Column("payout", Integer, nullable=False, server_default=text("0")),
    Column("profit", Integer, nullable=False, server_default=text("0")),
    Column("fetched_at", String(64), nullable=False, server_default=text("''")),
    Column("my_race_results", JSON, nullable=False, server_default=text("'[]'")),
    UniqueConstraint("date_key", "profile_id", name="uq_orepro_daily_results_date_profile"),
)

orepro_race_results_table = Table(
    "orepro_race_results",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("daily_result_id", Integer, ForeignKey("orepro_daily_results.id", ondelete="CASCADE"), nullable=False),
    Column("race_id", String(32), nullable=False, server_default=text("''")),
    Column("race_number", Integer, nullable=False, server_default=text("0")),
    Column("purchase", Integer, nullable=False, server_default=text("0")),
    Column("payout", Integer, nullable=False, server_default=text("0")),
    Column("profit", Integer, nullable=False, server_default=text("0")),
    Column("raw_payload", JSON, nullable=False, server_default=text("'{}'")),
    UniqueConstraint("daily_result_id", "race_id", name="uq_orepro_race_results_daily_race"),
)


def get_db_engine():
    global _db_engine
    if _db_engine is None:
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        _db_engine = create_engine(
            config.DB_URL,
            connect_args={"check_same_thread": False},
            future=True,
        )
    return _db_engine


def get_db_session_factory():
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_db_engine(),
            autoflush=False,
            autocommit=False,
            future=True,
        )
    return _session_factory


@contextmanager
def db_session_scope():
    session = get_db_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def bootstrap_schema():
    engine = get_db_engine()
    DB_METADATA.create_all(bind=engine)

    with engine.begin() as conn:
        version_exists = conn.execute(
            text("SELECT 1 FROM schema_migrations WHERE version = :version LIMIT 1"),
            {"version": CURRENT_SCHEMA_VERSION},
        ).first()

        if version_exists is None:
            conn.execute(schema_migrations_table.insert().values(version=CURRENT_SCHEMA_VERSION))
            logger.info("Applied schema version %s", CURRENT_SCHEMA_VERSION)


def get_latest_schema_version():
    engine = get_db_engine()
    with engine.connect() as conn:
        latest = conn.execute(text("SELECT MAX(version) FROM schema_migrations")).scalar_one_or_none()
    return int(latest or 0)


def init_storage_foundation():
    engine = get_db_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    bootstrap_schema()
    logger.info(
        "SQLite storage foundation ready at %s (schema v%s)",
        config.DB_FILE,
        get_latest_schema_version(),
    )


def atomic_write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=target.parent) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=4)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def atomic_write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=target.parent) as tmp:
        tmp.write(text)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def atomic_write_pickle(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", delete=False, dir=target.parent) as tmp:
        pickle.dump(payload, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, target)


def safe_read_json(path, default):
    target = Path(path)
    if not target.exists():
        return copy.deepcopy(default)
    try:
        with open(target, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read JSON from %s: %s", target, exc)
        return copy.deepcopy(default)


def load_text_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    return ""


def load_pickle(path, default=None):
    if not os.path.exists(path):
        return [] if default is None else default
    with open(path, "rb") as f:
        return pickle.load(f)


def _merge_dicts(defaults, override):
    if not isinstance(defaults, dict):
        return copy.deepcopy(override) if override is not None else copy.deepcopy(defaults)
    merged = copy.deepcopy(defaults)
    if not isinstance(override, dict):
        return merged
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def load_app_config(path=None):
    config_path = path or (config.DATA_DIR / "config.json")
    stored = safe_read_json(config_path, {})
    return _merge_dicts(APP_CONFIG_DEFAULTS, stored)


def save_app_config(config_data, path=None):
    config_path = path or (config.DATA_DIR / "config.json")
    atomic_write_json(config_path, config_data)