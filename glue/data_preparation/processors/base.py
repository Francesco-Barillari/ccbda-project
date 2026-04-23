from abc import ABC, abstractmethod
from typing import Dict, Optional


class BaseProcessor(ABC):
    """
    Extend this to support a new data source.
    1. Create processors/your_api.py with a subclass.
    2. Register it in processors/__init__.py.
    3. Add a source entry in config/pipeline.json.
    """

    @abstractmethod
    def parse(self, payload: dict) -> dict:
        """Normalize raw SQS message payload into a canonical record."""
        pass

    @abstractmethod
    def to_tts_row(self, record: dict) -> Dict:
        """Return a Target Time Series row: {item_id, timestamp, target_value}."""
        pass

    def to_rts_rows(self, record: dict) -> Optional[Dict[str, Dict]]:
        """
        Return a dict of {series_name: row_dict} for Related Time Series, or None.
        Each series_name becomes its own S3 prefix (e.g. 'volume', 'sentiment_score').
        Row must include item_id and timestamp.
        """
        return None

    @staticmethod
    def fmt_timestamp(ts: str) -> str:
        """Convert any ISO-8601 string to Forecast format: YYYY-MM-DD HH:MM:SS."""
        from datetime import datetime
        return datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")
