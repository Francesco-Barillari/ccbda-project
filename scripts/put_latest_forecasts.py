from storage.forecast_state_storage import ForecastState, ForecastStateStorage


def main() -> None: # la chiamo io da terminale
    storage = ForecastStateStorage() # creation of the object and binding with the dynamodb table in aws

    state = ForecastState( # creation of object (row in the table)
        asset="BTC",
        forecast_timestamp="2026-04-16T10:00:00Z",
        observed_timestamp="2026-04-16T10:05:00Z",
        current_price=81700.0,
        predicted_price=85000.0,
        lower_bound=83500.0,
        upper_bound=86300.0,
        sentiment_score=0.76,
        anomaly_flag=True,
        deviation_abs=3300.0,
        deviation_pct=3.88,
        alert_status="TRIGGERED", # this and anomaly flag will be set on the data by the check_anomaly 
    )

    storage.put_latest_state(state) # Insertion of the row
    print(f"Saved latest forecast state for asset: {state.asset}")

# I create one Python object for every get and every put but the aws table is the same.

if __name__ == "__main__":
    main()