from datetime import datetime, timezone
from processor.processor import parse_event, window_start


def test_window_is_five_minutes():
    value = datetime(2026, 1, 1, 12, 8, 30, tzinfo=timezone.utc)
    assert window_start(value).minute == 5


def test_invalid_event_rejected():
    try:
        parse_event(b'{"event_id":"x"}')
    except ValueError:
        return
    raise AssertionError("invalid event was accepted")
