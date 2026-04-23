from typing import Dict, Optional
from .base import BaseProcessor


class AlphaVantageProcessor(BaseProcessor):
    """Handles messages from the Alpha Vantage GLOBAL_QUOTE ingestion Lambda."""

    def parse(self, payload: dict) -> dict:
        return {
            "symbol":    payload["symbol"],
            "price":     float(payload["price"]),
            "volume":    int(payload["volume"]),
            "timestamp": payload["timestamp"],
        }

    def to_tts_row(self, record: dict) -> Dict:
        return {
            "item_id":      record["symbol"],
            "timestamp":    self.fmt_timestamp(record["timestamp"]),
            "target_value": record["price"],
        }

    def to_rts_rows(self, record: dict) -> Optional[Dict[str, Dict]]:
        ts = self.fmt_timestamp(record["timestamp"])
        return {
            "volume": {
                "item_id":   record["symbol"],
                "timestamp": ts,
                "volume":    record["volume"],
            }
        }
