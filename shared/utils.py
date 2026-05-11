from datetime import datetime, timezone


def utcnow() -> datetime:
    """Naive UTC datetime — compatible with SQLite columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
