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
    "backend": {
        "dataEngine": "nk",
    },
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
        "prefetchRaceCheck": False,
        "debugConsole": False,
        "autoLockPastVotes": False,
        "showConsole": True,
        "highlightFallbackBridge": False,
        "tvModeSplitPercent": 50,
        "tvModePanelsFlipped": False,
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
    Column("display_name", String(255), nullable=False, server_default=text("''")),
    Column("added_at", DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

watchlist_horses_table = Table(
    "watchlist_horses",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("horse_id", String(32), nullable=False, unique=True),
    Column("display_name", String(255), nullable=False, server_default=text("''")),
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

races_nk_table = Table(
    "races_nk",
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

race_entries_nk_table = Table(
    "race_entries_nk",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("race_id", String(32), ForeignKey("races_nk.race_id", ondelete="CASCADE"), nullable=False),
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
    UniqueConstraint("race_id", "horse_number", name="uq_race_entries_nk_race_horse_number"),
)

races_jv_table = Table(
    "races_jv",
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

race_entries_jv_table = Table(
    "race_entries_jv",
    DB_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("race_id", String(32), ForeignKey("races_jv.race_id", ondelete="CASCADE"), nullable=False),
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
    UniqueConstraint("race_id", "horse_number", name="uq_race_entries_jv_race_horse_number"),
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


def dispose_storage_connections():
    """Dispose cached SQLAlchemy engine/session factory.
    This is used before destructive file operations (like restore) so
    Windows can replace/delete the SQLite file without lock errors."""
    global _db_engine, _session_factory
    if _db_engine is not None:
        _db_engine.dispose()
    _db_engine = None
    _session_factory = None


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


def _apply_incremental_migrations(conn):
    """Apply in-place ALTER TABLE migrations for columns added after initial schema creation."""
    for table_name in ("tracked_horses", "watchlist_horses"):
        existing_cols = {
            row[1]
            for row in conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        }
        if "display_name" not in existing_cols:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN display_name TEXT NOT NULL DEFAULT ''"))
            logger.info("Added display_name column to %s", table_name)


def bootstrap_schema():
    engine = get_db_engine()
    DB_METADATA.create_all(bind=engine)

    with engine.begin() as conn:
        _apply_incremental_migrations(conn)

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


def _safe_int(value, default=0):
    try:
        text_value = str(value).strip()
        if text_value == "":
            return default
        return int(float(text_value))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        text_value = str(value).strip()
        if text_value == "":
            return default
        return float(text_value)
    except (TypeError, ValueError):
        return default


def _coerce_jsonable(value):
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and (value != value):
            return ""
        return value
    if isinstance(value, dict):
        return {str(k): _coerce_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_coerce_jsonable(v) for v in value]
    text_value = str(value)
    return "" if text_value.lower() == "nan" else text_value


def _entries_to_records(entries):
    if entries is None:
        return []
    try:
        import pandas as pd  # local import to avoid unnecessary module load at startup
        if isinstance(entries, pd.DataFrame):
            return [
                {str(k): _coerce_jsonable(v) for k, v in row.items()}
                for row in entries.to_dict(orient="records")
            ]
    except Exception:
        pass

    if isinstance(entries, list):
        return [
            {str(k): _coerce_jsonable(v) for k, v in row.items()}
            for row in entries
            if isinstance(row, dict)
        ]
    return []


def _write_race_cache_to_db(session, weekend_races):
    session.execute(race_entries_table.delete())
    session.execute(races_table.delete())

    race_rows = []
    entry_rows = []
    for race_order, race in enumerate(weekend_races or []):
        info = race.get("info", {}) if isinstance(race, dict) else {}
        race_id = str(info.get("race_id", "")).strip()
        if not race_id:
            continue

        info_payload = {
            str(k): _coerce_jsonable(v)
            for k, v in info.items()
            if isinstance(k, str)
        }
        info_payload["_cache_order"] = race_order

        race_rows.append(
            {
                "race_id": race_id,
                "race_date": str(info.get("clean_date") or "").strip(),
                "kaisai_id": str(info.get("kaisai_id") or "").strip(),
                "track": str(info.get("place") or "").strip(),
                "race_name": str(info.get("race_name") or "").strip(),
                "race_number": _safe_int(info.get("race_number"), 0),
                "sort_time": str(info.get("sort_time") or "").strip(),
                "distance": _safe_int(info.get("distance"), 0),
                "surface": str(info.get("surface") or "").strip(),
                "grade": str(info.get("grade") or "").strip(),
                "raw_payload": info_payload,
            }
        )

        used_horse_numbers = set()
        for row_order, entry in enumerate(_entries_to_records(race.get("entries"))):
            horse_number = _safe_int(entry.get("PP"), 0)
            if horse_number <= 0 or horse_number in used_horse_numbers:
                horse_number = 1000 + row_order
            used_horse_numbers.add(horse_number)

            entry_payload = {str(k): _coerce_jsonable(v) for k, v in entry.items()}
            entry_payload["_row_order"] = row_order

            entry_rows.append(
                {
                    "race_id": race_id,
                    "horse_id": str(entry.get("Horse_ID") or "").strip(),
                    "horse_name": str(entry.get("Horse") or "").strip(),
                    "frame_number": _safe_int(entry.get("BK"), 0),
                    "horse_number": horse_number,
                    "jockey": str(entry.get("Jockey") or "").strip(),
                    "odds": _safe_float(entry.get("Odds"), 0.0),
                    "finish_position": _safe_int(entry.get("Finish"), 0),
                    "entry_score": _safe_float(entry.get("Score"), 0.0),
                    "mark_symbol": str(entry.get("Match") or "").strip(),
                    "raw_payload": entry_payload,
                }
            )

    if race_rows:
        session.execute(races_table.insert(), race_rows)
    if entry_rows:
        session.execute(race_entries_table.insert(), entry_rows)


def normalize_data_engine(data_engine):
    engine = str(data_engine or "").strip().lower()
    return engine if engine in {"nk", "jv"} else "nk"


def _race_tables_for_engine(data_engine):
    engine = normalize_data_engine(data_engine)
    if engine == "jv":
        return races_jv_table, race_entries_jv_table
    return races_nk_table, race_entries_nk_table


def get_active_data_engine(default="nk"):
    cfg = load_app_config()
    backend = cfg.get("backend", {}) if isinstance(cfg, dict) else {}
    configured = backend.get("dataEngine", default)
    return normalize_data_engine(configured)


def _write_race_cache_to_engine_tables(session, weekend_races, races_tbl, entries_tbl):
    session.execute(entries_tbl.delete())
    session.execute(races_tbl.delete())

    race_rows = []
    entry_rows = []
    for race_order, race in enumerate(weekend_races or []):
        info = race.get("info", {}) if isinstance(race, dict) else {}
        race_id = str(info.get("race_id", "")).strip()
        if not race_id:
            continue

        info_payload = {
            str(k): _coerce_jsonable(v)
            for k, v in info.items()
            if isinstance(k, str)
        }
        info_payload["_cache_order"] = race_order

        race_rows.append(
            {
                "race_id": race_id,
                "race_date": str(info.get("clean_date") or "").strip(),
                "kaisai_id": str(info.get("kaisai_id") or "").strip(),
                "track": str(info.get("place") or "").strip(),
                "race_name": str(info.get("race_name") or "").strip(),
                "race_number": _safe_int(info.get("race_number"), 0),
                "sort_time": str(info.get("sort_time") or "").strip(),
                "distance": _safe_int(info.get("distance"), 0),
                "surface": str(info.get("surface") or "").strip(),
                "grade": str(info.get("grade") or "").strip(),
                "raw_payload": info_payload,
            }
        )

        used_horse_numbers = set()
        for row_order, entry in enumerate(_entries_to_records(race.get("entries"))):
            horse_number = _safe_int(entry.get("PP"), 0)
            if horse_number <= 0 or horse_number in used_horse_numbers:
                horse_number = 1000 + row_order
            used_horse_numbers.add(horse_number)

            entry_payload = {str(k): _coerce_jsonable(v) for k, v in entry.items()}
            entry_payload["_row_order"] = row_order

            entry_rows.append(
                {
                    "race_id": race_id,
                    "horse_id": str(entry.get("Horse_ID") or "").strip(),
                    "horse_name": str(entry.get("Horse") or "").strip(),
                    "frame_number": _safe_int(entry.get("BK"), 0),
                    "horse_number": horse_number,
                    "jockey": str(entry.get("Jockey") or "").strip(),
                    "odds": _safe_float(entry.get("Odds"), 0.0),
                    "finish_position": _safe_int(entry.get("Finish"), 0),
                    "entry_score": _safe_float(entry.get("Score"), 0.0),
                    "mark_symbol": str(entry.get("Match") or "").strip(),
                    "raw_payload": entry_payload,
                }
            )

    if race_rows:
        session.execute(races_tbl.insert(), race_rows)
    if entry_rows:
        session.execute(entries_tbl.insert(), entry_rows)


def load_race_cache(data_engine="nk"):
    races_tbl, entries_tbl = _race_tables_for_engine(data_engine)
    with db_session_scope() as session:
        race_rows = session.execute(races_tbl.select()).all()
        if not race_rows:
            return []

        entries_by_race = {}
        for row in session.execute(entries_tbl.select()).all():
            entries_by_race.setdefault(row.race_id, []).append(row)

        try:
            import pandas as pd
        except Exception as exc:
            logger.error("Failed to import pandas while loading race cache: %s", exc)
            return []

        ordered_races = sorted(
            race_rows,
            key=lambda row: (
                _safe_int((row.raw_payload or {}).get("_cache_order"), 999999),
                str(row.race_date or ""),
                str(row.sort_time or ""),
                _safe_int(row.race_number, 0),
                str(row.race_id or ""),
            ),
        )

        weekend_races = []
        for race_row in ordered_races:
            info_payload = race_row.raw_payload if isinstance(race_row.raw_payload, dict) else {}
            info = dict(info_payload) if info_payload else {
                "race_id": race_row.race_id,
                "clean_date": race_row.race_date,
                "place": race_row.track,
                "race_name": race_row.race_name,
                "race_number": race_row.race_number,
                "sort_time": race_row.sort_time,
            }

            entry_rows = entries_by_race.get(race_row.race_id, [])
            ordered_entries = sorted(
                entry_rows,
                key=lambda row: (_safe_int((row.raw_payload or {}).get("_row_order"), 999999), row.id),
            )
            entry_records = [
                dict(row.raw_payload) if isinstance(row.raw_payload, dict) else {}
                for row in ordered_entries
            ]

            weekend_races.append(
                {
                    "info": info,
                    "entries": pd.DataFrame(entry_records),
                }
            )

        return weekend_races


def save_race_cache(weekend_races, data_engine="nk"):
    races_tbl, entries_tbl = _race_tables_for_engine(data_engine)
    with db_session_scope() as session:
        _write_race_cache_to_engine_tables(session, weekend_races, races_tbl, entries_tbl)


def clear_race_cache(data_engine="nk", clear_all=False):
    with db_session_scope() as session:
        if clear_all:
            session.execute(race_entries_nk_table.delete())
            session.execute(races_nk_table.delete())
            session.execute(race_entries_jv_table.delete())
            session.execute(races_jv_table.delete())
            return

        races_tbl, entries_tbl = _race_tables_for_engine(data_engine)
        session.execute(entries_tbl.delete())
        session.execute(races_tbl.delete())


def _load_legacy_marks_store_from_file(path):
    raw = safe_read_json(path, {})
    if not raw:
        return {"version": 2, "marks": {}, "raceMeta": {}}

    is_versioned = any(k in raw for k in ("version", "marks", "raceMeta"))
    raw_marks = raw.get("marks", {}) if is_versioned else raw
    raw_meta = raw.get("raceMeta", {}) if is_versioned else {}

    marks = {str(k).strip(): str(v).strip() for k, v in raw_marks.items() if k and v}
    race_meta = {}
    for race_id, meta in (raw_meta.items() if isinstance(raw_meta, dict) else []):
        if not isinstance(meta, dict):
            continue
        race_meta[str(race_id).strip()] = {
            "savedAt": str(meta.get("savedAt") or "").strip() or None,
            "updatedAt": str(meta.get("updatedAt") or "").strip() or None,
            "markSource": str(meta.get("markSource") or "").strip() or None,
            "strategySnapshot": meta.get("strategySnapshot") if isinstance(meta.get("strategySnapshot"), dict) else {},
            "manualAdjustments": int(meta.get("manualAdjustments") or 0),
            "lockStateAtSave": meta.get("lockStateAtSave"),
            "activeSymbols": meta.get("activeSymbols") if isinstance(meta.get("activeSymbols"), list) else [],
        }
    return {"version": 2, "marks": marks, "raceMeta": race_meta}


def _import_legacy_orepro_files_if_present(session):
    history_path = config.DATA_DIR / "orepro_results_history.json"
    last_sync_path = config.DATA_DIR / "orepro_last_sync.json"

    history = safe_read_json(history_path, {"entries": []})
    entries = history.get("entries", []) if isinstance(history, dict) else []
    if not isinstance(entries, list):
        entries = []

    imported = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        _orepro_write_history_entry(session, entry)
        imported += 1

    if imported == 0:
        last_payload = safe_read_json(last_sync_path, {})
        seeded_entry = _orepro_build_history_entry_from_payload(last_payload)
        if seeded_entry is not None:
            _orepro_write_history_entry(session, seeded_entry)
            imported = 1

    return imported


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


def _upsert_app_config(session, config_data):
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    stmt = (
        sqlite_insert(app_config_table)
        .values(config_key="app_config", config_value=config_data)
        .on_conflict_do_update(
            index_elements=["config_key"],
            set_={"config_value": config_data, "updated_at": text("CURRENT_TIMESTAMP")},
        )
    )
    session.execute(stmt)


def load_app_config(path=None):
    """Load app config from DB.
    The optional `path` argument is accepted for backward compatibility."""
    with db_session_scope() as session:
        row = session.execute(
            app_config_table.select().where(app_config_table.c.config_key == "app_config")
        ).first()
        if row is not None:
            return _merge_dicts(APP_CONFIG_DEFAULTS, row.config_value)
        return copy.deepcopy(APP_CONFIG_DEFAULTS)


def save_app_config(config_data, path=None):
    """Save app config to DB. The optional `path` argument is accepted for
    backward compatibility but ignored now that config is DB-backed."""
    with db_session_scope() as session:
        _upsert_app_config(session, config_data)


# --- Horse list repositories ---

def _parse_horse_lines_from_text(raw_text):
    """Extract (horse_id, display_name) pairs from raw list text (format: ID # Name)."""
    results = []
    seen = set()
    for line in (raw_text or "").splitlines():
        parts = line.split("#", 1)
        horse_id = parts[0].strip()
        if horse_id and len(horse_id) == 10 and horse_id not in seen:
            name = parts[1].strip() if len(parts) > 1 else ""
            results.append((horse_id, name))
            seen.add(horse_id)
    return results


def load_horse_list(list_type):
    """Load (horse_id, display_name) pairs for 'favorites' or 'watchlist' from DB.
    Returns an ordered list of (horse_id, display_name) tuples."""
    if list_type == "favorites":
        table = tracked_horses_table
    else:
        table = watchlist_horses_table
    with db_session_scope() as session:
        rows = session.execute(table.select().order_by(table.c.added_at)).all()
        return [(row.horse_id, getattr(row, 'display_name', '')) for row in rows]


def save_horse_list(list_type, horse_ids):
    """Replace all DB entries for the given list with the provided horse ID list.
    Accepts either a list of horse_id strings or (horse_id, display_name) tuples."""
    table = tracked_horses_table if list_type == "favorites" else watchlist_horses_table
    with db_session_scope() as session:
        session.execute(table.delete())
        if horse_ids:
            rows = []
            for item in horse_ids:
                if isinstance(item, tuple):
                    rows.append({"horse_id": item[0], "display_name": item[1] if len(item) > 1 else ""})
                else:
                    rows.append({"horse_id": item, "display_name": ""})
            session.execute(table.insert(), rows)


def add_horse_to_list(list_type, horse_id, display_name=""):
    """Add a single horse to the given list. Silent no-op if already present."""
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    table = tracked_horses_table if list_type == "favorites" else watchlist_horses_table
    with db_session_scope() as session:
        stmt = (
            sqlite_insert(table)
            .values(horse_id=horse_id, display_name=display_name)
            .on_conflict_do_nothing(index_elements=["horse_id"])
        )
        session.execute(stmt)


def horse_ids_to_text(horse_pairs):
    """Reconstruct the wire-format text (ID # Name per line) from a list of
    (horse_id, display_name) tuples. Bare ID strings are also accepted."""
    if not horse_pairs:
        return ""
    lines = []
    for item in horse_pairs:
        if isinstance(item, tuple):
            horse_id, name = item[0], item[1] if len(item) > 1 else ""
        else:
            horse_id, name = item, ""
        lines.append(f"{horse_id} # {name}" if name else horse_id)
    return "\n".join(lines) + "\n"


# --- Horse cache repositories ---

def _normalize_horse_cache_entry(entry):
    base = entry if isinstance(entry, dict) else {}
    return {
        "name": str(base.get("name") or "").strip(),
        "sire": str(base.get("sire") or "").strip(),
        "dam": str(base.get("dam") or "").strip(),
        "bms": str(base.get("bms") or "").strip(),
        "sire_id": str(base.get("sire_id") or "").strip(),
        "dam_id": str(base.get("dam_id") or "").strip(),
        "bms_id": str(base.get("bms_id") or "").strip(),
        "record": str(base.get("record") or "0/0").strip() or "0/0",
    }


def _horse_cache_row_to_entry(row):
    raw_payload = row.raw_payload if isinstance(row.raw_payload, dict) else {}
    return {
        "name": row.horse_name or "",
        "sire": row.sire or "",
        "dam": row.dam or "",
        "bms": row.bms or "",
        "sire_id": str(raw_payload.get("sire_id") or "").strip(),
        "dam_id": str(raw_payload.get("dam_id") or "").strip(),
        "bms_id": str(raw_payload.get("bms_id") or "").strip(),
        "record": str(raw_payload.get("record") or "0/0").strip() or "0/0",
    }


def _upsert_horse_cache_entry(session, horse_id, entry):
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    clean_horse_id = str(horse_id or "").replace(".0", "").strip()
    if not clean_horse_id:
        return

    normalized = _normalize_horse_cache_entry(entry)
    pedigree_payload = {
        "sire": normalized["sire"],
        "dam": normalized["dam"],
        "bms": normalized["bms"],
        "sire_id": normalized["sire_id"],
        "dam_id": normalized["dam_id"],
        "bms_id": normalized["bms_id"],
    }
    raw_payload = {
        "sire_id": normalized["sire_id"],
        "dam_id": normalized["dam_id"],
        "bms_id": normalized["bms_id"],
        "record": normalized["record"],
    }

    stmt = (
        sqlite_insert(horses_table)
        .values(
            horse_id=clean_horse_id,
            horse_name=normalized["name"],
            sire=normalized["sire"],
            dam=normalized["dam"],
            bms=normalized["bms"],
            pedigree=pedigree_payload,
            raw_payload=raw_payload,
        )
        .on_conflict_do_update(
            index_elements=["horse_id"],
            set_={
                "horse_name": normalized["name"],
                "sire": normalized["sire"],
                "dam": normalized["dam"],
                "bms": normalized["bms"],
                "pedigree": pedigree_payload,
                "raw_payload": raw_payload,
                "updated_at": text("CURRENT_TIMESTAMP"),
            },
        )
    )
    session.execute(stmt)


def load_horse_cache_map():
    with db_session_scope() as session:
        rows = session.execute(horses_table.select()).all()
        return {row.horse_id: _horse_cache_row_to_entry(row) for row in rows if row.horse_id}


def upsert_horse_cache_entry(horse_id, entry):
    with db_session_scope() as session:
        _upsert_horse_cache_entry(session, horse_id, entry)


def upsert_horse_cache_entries(entries_by_id):
    if not isinstance(entries_by_id, dict) or not entries_by_id:
        return
    with db_session_scope() as session:
        for horse_id, entry in entries_by_id.items():
            _upsert_horse_cache_entry(session, horse_id, entry)


def clear_horse_cache_entries():
    with db_session_scope() as session:
        session.execute(horses_table.delete())


def count_horse_cache_entries():
    with db_session_scope() as session:
        return int(session.execute(text("SELECT COUNT(1) FROM horses")).scalar_one() or 0)


def delete_horse_cache_entries_by_ids(horse_ids):
    clean_ids = [str(h).strip() for h in (horse_ids or []) if str(h).strip()]
    if not clean_ids:
        return 0
    with db_session_scope() as session:
        result = session.execute(
            horses_table.delete().where(horses_table.c.horse_id.in_(clean_ids))
        )
        return int(result.rowcount or 0)


# --- Marks and race metadata repositories ---

def load_marks_store():
    """Load the full marks store from DB in the canonical {version, marks, raceMeta} shape."""
    with db_session_scope() as session:
        mark_rows = session.execute(race_marks_table.select()).all()
        meta_rows = session.execute(race_metadata_table.select()).all()

        if mark_rows or meta_rows:
            marks = {row.race_id + "_" + row.horse_key: row.mark_symbol for row in mark_rows}
            race_meta = {}
            for row in meta_rows:
                race_meta[row.race_id] = {
                    "savedAt": row.saved_at or None,
                    "updatedAt": row.updated_at or None,
                    "markSource": row.mark_source or None,
                    "strategySnapshot": row.strategy_snapshot if isinstance(row.strategy_snapshot, dict) else {},
                    "manualAdjustments": row.manual_adjustments or 0,
                    "lockStateAtSave": row.lock_state_at_save,
                    "activeSymbols": row.active_symbols if isinstance(row.active_symbols, list) else [],
                }
            return {"version": 2, "marks": marks, "raceMeta": race_meta}

        return {"version": 2, "marks": {}, "raceMeta": {}}


def _write_marks_store_to_db(session, store):
    """Overwrite DB marks and race_metadata from the given store dict."""
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    marks = store.get("marks", {})
    race_meta = store.get("raceMeta", {})

    # Rebuild race_marks: clear and reinsert
    session.execute(race_marks_table.delete())
    if marks:
        rows = []
        for composite_key, symbol in marks.items():
            if not symbol:
                continue
            parts = composite_key.split("_", 1)
            if len(parts) != 2:
                continue
            rows.append({"race_id": parts[0], "horse_key": parts[1], "mark_symbol": symbol})
        if rows:
            session.execute(race_marks_table.insert(), rows)

    # Rebuild race_metadata: clear and reinsert
    session.execute(race_metadata_table.delete())
    if race_meta:
        rows = []
        for race_id, meta in race_meta.items():
            if not isinstance(meta, dict):
                continue
            rows.append({
                "race_id": race_id,
                "saved_at": str(meta.get("savedAt") or ""),
                "updated_at": str(meta.get("updatedAt") or ""),
                "mark_source": str(meta.get("markSource") or ""),
                "strategy_snapshot": meta.get("strategySnapshot") or {},
                "manual_adjustments": int(meta.get("manualAdjustments") or 0),
                "lock_state_at_save": meta.get("lockStateAtSave"),
                "active_symbols": meta.get("activeSymbols") or [],
            })
        if rows:
            session.execute(race_metadata_table.insert(), rows)


def save_marks_store(store):
    """Persist the full marks store to DB."""
    with db_session_scope() as session:
        _write_marks_store_to_db(session, store)


def delete_marks_for_races(race_ids):
    """Remove all marks and metadata rows for the given set of race_ids."""
    race_ids = list(race_ids)
    if not race_ids:
        return 0, 0
    with db_session_scope() as session:
        deleted_marks = 0
        deleted_meta = 0
        for rid in race_ids:
            r = session.execute(
                race_marks_table.delete().where(race_marks_table.c.race_id == rid)
            )
            deleted_marks += r.rowcount
            r = session.execute(
                race_metadata_table.delete().where(race_metadata_table.c.race_id == rid)
            )
            deleted_meta += r.rowcount
    return deleted_marks, deleted_meta


# --- OrePro repositories ---

def _format_orepro_yen(value):
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,}円"


def _ensure_orepro_profile(session, profile_id, username=""):
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    pid = str(profile_id or "").strip()
    if not pid:
        pid = "unknown"
    uname = str(username or "").strip()

    stmt = (
        sqlite_insert(orepro_profiles_table)
        .values(profile_id=pid, username=uname)
        .on_conflict_do_update(
            index_elements=["profile_id"],
            set_={"username": uname, "updated_at": text("CURRENT_TIMESTAMP")},
        )
    )
    session.execute(stmt)
    return pid


def _orepro_build_history_entry_from_payload(payload):
    if not isinstance(payload, dict):
        return None
    summary = payload.get("myBetSummary") or {}
    date_key = str(payload.get("kaisai_date") or "").strip()
    profile_id = str(((payload.get("debug") or {}).get("yosokaIdUsed") or payload.get("memberId") or "")).strip()
    races = int(summary.get("races") or 0)
    if not date_key or not profile_id or races <= 0:
        return None

    purchase = int(summary.get("purchase") or 0)
    payout = int(summary.get("payout") or 0)
    profit = int(summary.get("profit") or 0)
    return {
        "date": date_key,
        "profileId": profile_id,
        "username": str(payload.get("username") or ((payload.get("debug") or {}).get("username") or "")).strip(),
        "kaisaiId": str(payload.get("kaisai_id") or "").strip(),
        "resolvedKaisaiIds": payload.get("resolvedKaisaiIds") or [],
        "isPartial": bool(payload.get("kaisai_id")),
        "races": races,
        "purchase": purchase,
        "purchaseLabel": _format_orepro_yen(purchase),
        "payout": payout,
        "payoutLabel": _format_orepro_yen(payout),
        "profit": profit,
        "profitLabel": _format_orepro_yen(profit),
        "fetchedAt": str(payload.get("fetchedAt") or ""),
        "myRaceResults": payload.get("myRaceResults") or [],
    }


def _orepro_should_replace_history_entry(existing, incoming):
    if not isinstance(existing, dict):
        return True
    existing_partial = bool(existing.get("isPartial"))
    incoming_partial = bool(incoming.get("isPartial"))
    if existing_partial and not incoming_partial:
        return True
    if not existing_partial and incoming_partial:
        return False

    existing_races = int(existing.get("races") or 0)
    incoming_races = int(incoming.get("races") or 0)
    if incoming_races != existing_races:
        return incoming_races > existing_races

    return str(incoming.get("fetchedAt") or "") >= str(existing.get("fetchedAt") or "")


def _orepro_summary_from_entries(entries):
    valid_entries = [entry for entry in entries if isinstance(entry, dict)]
    sorted_entries = sorted(
        valid_entries,
        key=lambda item: (str(item.get("date") or ""), str(item.get("fetchedAt") or "")),
        reverse=True,
    )
    purchase = sum(int(entry.get("purchase") or 0) for entry in sorted_entries)
    payout = sum(int(entry.get("payout") or 0) for entry in sorted_entries)
    profit = sum(int(entry.get("profit") or 0) for entry in sorted_entries)
    races = sum(int(entry.get("races") or 0) for entry in sorted_entries)
    roi_pct = round((payout / purchase) * 100, 1) if purchase > 0 else 0.0
    best_day = max(sorted_entries, key=lambda entry: int(entry.get("profit") or 0), default=None)
    worst_day = min(sorted_entries, key=lambda entry: int(entry.get("profit") or 0), default=None)

    return {
        "status": "success",
        "entries": sorted_entries,
        "totals": {
            "days": len(sorted_entries),
            "races": races,
            "purchase": purchase,
            "purchaseLabel": _format_orepro_yen(purchase),
            "payout": payout,
            "payoutLabel": _format_orepro_yen(payout),
            "profit": profit,
            "profitLabel": _format_orepro_yen(profit),
            "roiPct": roi_pct,
            "bestDay": best_day,
            "worstDay": worst_day,
            "lastUpdatedAt": sorted_entries[0].get("fetchedAt", "") if sorted_entries else "",
        },
    }


def _orepro_write_history_entry(session, entry):
    profile_id = _ensure_orepro_profile(session, entry.get("profileId"), entry.get("username"))
    date_key = str(entry.get("date") or "").strip()
    if not date_key:
        return

    existing_row = session.execute(
        orepro_daily_results_table.select().where(
            orepro_daily_results_table.c.date_key == date_key,
            orepro_daily_results_table.c.profile_id == profile_id,
        )
    ).first()

    existing_entry = None
    if existing_row is not None:
        existing_entry = {
            "isPartial": bool(existing_row.is_partial),
            "races": int(existing_row.races or 0),
            "fetchedAt": str(existing_row.fetched_at or ""),
        }
        if not _orepro_should_replace_history_entry(existing_entry, entry):
            return

    payload = {
        "date_key": date_key,
        "profile_id": profile_id,
        "username": str(entry.get("username") or "").strip(),
        "kaisai_id": str(entry.get("kaisaiId") or "").strip(),
        "resolved_kaisai_ids": entry.get("resolvedKaisaiIds") or [],
        "is_partial": bool(entry.get("isPartial")),
        "races": int(entry.get("races") or 0),
        "purchase": int(entry.get("purchase") or 0),
        "payout": int(entry.get("payout") or 0),
        "profit": int(entry.get("profit") or 0),
        "fetched_at": str(entry.get("fetchedAt") or ""),
        "my_race_results": entry.get("myRaceResults") or [],
    }

    if existing_row is None:
        session.execute(orepro_daily_results_table.insert().values(**payload))
        daily_row = session.execute(
            orepro_daily_results_table.select().where(
                orepro_daily_results_table.c.date_key == date_key,
                orepro_daily_results_table.c.profile_id == profile_id,
            )
        ).first()
    else:
        session.execute(
            orepro_daily_results_table.update()
            .where(orepro_daily_results_table.c.id == existing_row.id)
            .values(**payload)
        )
        daily_row = session.execute(
            orepro_daily_results_table.select().where(orepro_daily_results_table.c.id == existing_row.id)
        ).first()

    if daily_row is None:
        return

    session.execute(
        orepro_race_results_table.delete().where(orepro_race_results_table.c.daily_result_id == daily_row.id)
    )
    race_rows = []
    for row in (entry.get("myRaceResults") or []):
        if not isinstance(row, dict):
            continue
        race_rows.append(
            {
                "daily_result_id": daily_row.id,
                "race_id": str(row.get("raceId") or "").strip(),
                "race_number": int(row.get("raceNumber") or 0),
                "purchase": int(row.get("purchase") or 0),
                "payout": int(row.get("payout") or 0),
                "profit": int(row.get("profit") or 0),
                "raw_payload": row,
            }
        )
    if race_rows:
        session.execute(orepro_race_results_table.insert(), race_rows)


def _load_orepro_history_entries_from_db(session):
    daily_rows = session.execute(orepro_daily_results_table.select()).all()
    if not daily_rows:
        return []

    race_rows = session.execute(orepro_race_results_table.select()).all()
    races_by_daily = {}
    for row in race_rows:
        races_by_daily.setdefault(row.daily_result_id, []).append(row)

    entries = []
    for row in daily_rows:
        race_results = []
        for rr in races_by_daily.get(row.id, []):
            raw = rr.raw_payload if isinstance(rr.raw_payload, dict) else {}
            race_results.append(
                {
                    "raceId": str(rr.race_id or "").strip(),
                    "purchase": int(rr.purchase or 0),
                    "purchaseLabel": raw.get("purchaseLabel") or _format_orepro_yen(int(rr.purchase or 0)),
                    "payout": int(rr.payout or 0),
                    "payoutLabel": raw.get("payoutLabel") or _format_orepro_yen(int(rr.payout or 0)),
                    "profit": int(rr.profit or 0),
                    "profitLabel": raw.get("profitLabel") or _format_orepro_yen(int(rr.profit or 0)),
                }
            )
        entries.append(
            {
                "date": str(row.date_key or ""),
                "profileId": str(row.profile_id or ""),
                "username": str(row.username or ""),
                "kaisaiId": str(row.kaisai_id or ""),
                "resolvedKaisaiIds": row.resolved_kaisai_ids if isinstance(row.resolved_kaisai_ids, list) else [],
                "isPartial": bool(row.is_partial),
                "races": int(row.races or 0),
                "purchase": int(row.purchase or 0),
                "purchaseLabel": _format_orepro_yen(int(row.purchase or 0)),
                "payout": int(row.payout or 0),
                "payoutLabel": _format_orepro_yen(int(row.payout or 0)),
                "profit": int(row.profit or 0),
                "profitLabel": _format_orepro_yen(int(row.profit or 0)),
                "fetchedAt": str(row.fetched_at or ""),
                "myRaceResults": race_results,
            }
        )
    return entries


def orepro_upsert_history_from_payload(payload):
    """Upsert one history entry derived from a sync payload, then return summary."""
    with db_session_scope() as session:
        entry = _orepro_build_history_entry_from_payload(payload)
        if entry is not None:
            _orepro_write_history_entry(session, entry)
        entries = _load_orepro_history_entries_from_db(session)
    return _orepro_summary_from_entries(entries)


def orepro_get_history_summary():
    with db_session_scope() as session:
        entries = _load_orepro_history_entries_from_db(session)
    return _orepro_summary_from_entries(entries)


def orepro_get_last_sync_payload():
    """Return a best-effort last sync payload derived from the latest DB history entry."""
    history = orepro_get_history_summary()
    entries = history.get("entries", []) if isinstance(history, dict) else []
    if not entries:
        return {
            "status": "idle",
            "loggedIn": False,
            "message": "No OrePro sync has been run yet.",
            "summaryLines": [],
            "yenValues": [],
            "historySummary": history,
        }


    last = entries[0]
    return {
        "status": "success",
        "loggedIn": True,
        "username": last.get("username", ""),
        "message": "Loaded most recent OrePro sync from DB history.",
        "fetchedAt": last.get("fetchedAt", ""),
        "kaisai_date": last.get("date", ""),
        "kaisai_id": last.get("kaisaiId", ""),
        "resolvedKaisaiIds": last.get("resolvedKaisaiIds", []),
        "raceIds": [str(row.get("raceId") or "") for row in (last.get("myRaceResults") or []) if row.get("raceId")],
        "myBetSummary": {
            "races": int(last.get("races") or 0),
            "purchase": int(last.get("purchase") or 0),
            "purchaseLabel": last.get("purchaseLabel") or _format_orepro_yen(int(last.get("purchase") or 0)),
            "payout": int(last.get("payout") or 0),
            "payoutLabel": last.get("payoutLabel") or _format_orepro_yen(int(last.get("payout") or 0)),
            "profit": int(last.get("profit") or 0),
            "profitLabel": last.get("profitLabel") or _format_orepro_yen(int(last.get("profit") or 0)),
        },
        "myRaceResults": last.get("myRaceResults") or [],
        "summaryLines": [],
        "yenValues": [],
        "historySummary": history,
        "debug": {"yosokaIdUsed": last.get("profileId", "")},
    }


def import_legacy_storage(overwrite_existing=False):
    """Explicitly import deprecated file-based storage into SQLite.
    This is intended for one-off recovery/migration, not normal runtime reads."""
    results = {
        "config": False,
        "favorites": 0,
        "watchlist": 0,
        "marks": 0,
        "raceMeta": 0,
        "horses": 0,
        "races": 0,
        "oreproDays": 0,
    }

    with db_session_scope() as session:
        config_payload = safe_read_json(config.DATA_DIR / "config.json", {})
        has_config = session.execute(
            app_config_table.select().where(app_config_table.c.config_key == "app_config")
        ).first() is not None
        if isinstance(config_payload, dict) and (overwrite_existing or (config_payload and not has_config)):
            _upsert_app_config(session, _merge_dicts(APP_CONFIG_DEFAULTS, config_payload))
            results["config"] = True

        tracked_pairs = _parse_horse_lines_from_text(load_text_file(config.TRACKING_FILE))
        existing_tracked = session.execute(text("SELECT COUNT(1) FROM tracked_horses")).scalar_one()
        if tracked_pairs and (overwrite_existing or int(existing_tracked or 0) == 0):
            session.execute(tracked_horses_table.delete())
            session.execute(tracked_horses_table.insert(), [{"horse_id": h, "display_name": n} for h, n in tracked_pairs])
            results["favorites"] = len(tracked_pairs)

        watch_pairs = _parse_horse_lines_from_text(load_text_file(config.WATCHLIST_FILE))
        existing_watch = session.execute(text("SELECT COUNT(1) FROM watchlist_horses")).scalar_one()
        if watch_pairs and (overwrite_existing or int(existing_watch or 0) == 0):
            session.execute(watchlist_horses_table.delete())
            session.execute(watchlist_horses_table.insert(), [{"horse_id": h, "display_name": n} for h, n in watch_pairs])
            results["watchlist"] = len(watch_pairs)

        marks_store = _load_legacy_marks_store_from_file(config.MARKS_FILE)
        existing_marks = session.execute(text("SELECT COUNT(1) FROM race_marks")).scalar_one()
        existing_meta = session.execute(text("SELECT COUNT(1) FROM race_metadata")).scalar_one()
        if (marks_store["marks"] or marks_store["raceMeta"]) and (
            overwrite_existing or (int(existing_marks or 0) == 0 and int(existing_meta or 0) == 0)
        ):
            _write_marks_store_to_db(session, marks_store)
            results["marks"] = len(marks_store["marks"])
            results["raceMeta"] = len(marks_store["raceMeta"])

        horse_cache = safe_read_json(config.HORSE_DICT_FILE, {})
        existing_horses = session.execute(text("SELECT COUNT(1) FROM horses")).scalar_one()
        if isinstance(horse_cache, dict) and horse_cache and (overwrite_existing or int(existing_horses or 0) == 0):
            if overwrite_existing:
                session.execute(horses_table.delete())
            imported_horses = 0
            for horse_id, payload in horse_cache.items():
                clean_horse_id = str(horse_id or "").replace(".0", "").strip()
                if not clean_horse_id:
                    continue
                _upsert_horse_cache_entry(session, clean_horse_id, payload)
                imported_horses += 1
            results["horses"] = imported_horses

        legacy_races = load_pickle(config.CACHE_FILE, [])
        existing_races = session.execute(text("SELECT COUNT(1) FROM races_nk")).scalar_one()
        if isinstance(legacy_races, list) and legacy_races and (overwrite_existing or int(existing_races or 0) == 0):
            _write_race_cache_to_engine_tables(session, legacy_races, races_nk_table, race_entries_nk_table)
            results["races"] = len(legacy_races)

        existing_orepro = session.execute(text("SELECT COUNT(1) FROM orepro_daily_results")).scalar_one()
        if overwrite_existing or int(existing_orepro or 0) == 0:
            if overwrite_existing:
                session.execute(orepro_race_results_table.delete())
                session.execute(orepro_daily_results_table.delete())
                session.execute(orepro_profiles_table.delete())
            results["oreproDays"] = _import_legacy_orepro_files_if_present(session)

    return results


def build_legacy_export_payloads():
    """Return a mapping of legacy file names to serialized bytes for recovery export."""
    payloads = {}

    payloads["data/config.json"] = json.dumps(load_app_config(), ensure_ascii=False, indent=4).encode("utf-8")
    payloads["data/tracked_horses.txt"] = horse_ids_to_text(load_horse_list("favorites")).encode("utf-8")
    payloads["data/watchlist_horses.txt"] = horse_ids_to_text(load_horse_list("watchlist")).encode("utf-8")
    payloads["data/saved_marks.json"] = json.dumps(load_marks_store(), ensure_ascii=False, indent=4).encode("utf-8")
    payloads["data/horse_names.json"] = json.dumps(load_horse_cache_map(), ensure_ascii=False, indent=4).encode("utf-8")
    payloads["data/race_cache.pkl"] = pickle.dumps(load_race_cache(data_engine="nk"))
    payloads["data/race_cache_nk.pkl"] = pickle.dumps(load_race_cache(data_engine="nk"))
    payloads["data/race_cache_jv.pkl"] = pickle.dumps(load_race_cache(data_engine="jv"))

    orepro_history = orepro_get_history_summary()
    payloads["data/orepro_results_history.json"] = json.dumps(
        {"entries": orepro_history.get("entries", [])}, ensure_ascii=False, indent=4
    ).encode("utf-8")
    payloads["data/orepro_last_sync.json"] = json.dumps(
        orepro_get_last_sync_payload(), ensure_ascii=False, indent=4
    ).encode("utf-8")
    payloads["data/orepro_session.json"] = json.dumps(
        {"nkauth": "", "updatedAt": ""}, ensure_ascii=False, indent=4
    ).encode("utf-8")
    return payloads