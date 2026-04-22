from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import boto3
from pydantic import BaseModel
from dotenv import load_dotenv


load_dotenv()

# definition of the object (row of the table)
class ForecastState(BaseModel):
    asset: str
    forecast_timestamp: str
    observed_timestamp: str
    current_price: float
    predicted_price: float
    lower_bound: float
    upper_bound: float
    sentiment_score: float
    anomaly_flag: bool
    deviation_abs: float
    deviation_pct: float
    alert_status: str

# constructor of the class: creation of the object table and binding new object - table in aws profile
class ForecastStateStorage:
    def __init__(self, table_name: str | None = None, region_name: str | None = None) -> None: # invoked when an object is created
        # can be created with table or region name or both or nothing
        self.table_name = table_name or os.getenv("DDB_TABLE_NAME") # it sets the table name with the one in the env or with the parameter
        self.region_name = region_name or os.getenv("AWS_DEFAULT_REGION", "eu-west-1") # idem

        if not self.table_name:
            raise ValueError("DDB_TABLE_NAME is not set")

        session_kwargs: dict[str, Any] = {} # it creates a new empty dictionary
        profile = os.getenv("AWS_PROFILE") # it tries to take the profile from the env
        if profile: # if the profile name exists it is set in the dictionary
            session_kwargs["profile_name"] = profile

        session = boto3.Session(**session_kwargs) # it creates a session AWS configured
        dynamodb = session.resource("dynamodb", region_name=self.region_name) # use the session in that region
        self._table = dynamodb.Table(self.table_name) # connect this object table with the one taken in the aws session

    @staticmethod
    def _to_decimal(value: float) -> Decimal:
        return Decimal(str(value))


    def put_latest_state(self, state: ForecastState) -> None: # invoked by the put_latest_forecast
        item = {
            "asset": state.asset,
            "forecast_timestamp": state.forecast_timestamp,
            "observed_timestamp": state.observed_timestamp,
            "current_price": self._to_decimal(state.current_price),
            "predicted_price": self._to_decimal(state.predicted_price),
            "lower_bound": self._to_decimal(state.lower_bound),
            "upper_bound": self._to_decimal(state.upper_bound),
            "sentiment_score": self._to_decimal(state.sentiment_score),
            "anomaly_flag": state.anomaly_flag,
            "deviation_abs": self._to_decimal(state.deviation_abs),
            "deviation_pct": self._to_decimal(state.deviation_pct),
            "alert_status": state.alert_status,
        }

        self._table.put_item(Item=item) # function (put_item) of boto3 library defined on the object self._table of boto3 library

    # invoked by the get_latest_forecasts
    def get_latest_state(self, asset: str) -> ForecastState | None:
        response = self._table.get_item(Key={"asset": asset}) # function of boto3 library (get_item)
        raw_item = response.get("Item")

        if not raw_item:
            return None

        return ForecastState(
            asset=str(raw_item["asset"]),
            forecast_timestamp=str(raw_item["forecast_timestamp"]),
            observed_timestamp=str(raw_item["observed_timestamp"]),
            current_price=float(raw_item["current_price"]),
            predicted_price=float(raw_item["predicted_price"]),
            lower_bound=float(raw_item["lower_bound"]),
            upper_bound=float(raw_item["upper_bound"]),
            sentiment_score=float(raw_item["sentiment_score"]),
            anomaly_flag=bool(raw_item["anomaly_flag"]),
            deviation_abs=float(raw_item["deviation_abs"]),
            deviation_pct=float(raw_item["deviation_pct"]),
            alert_status=str(raw_item["alert_status"]),
        )