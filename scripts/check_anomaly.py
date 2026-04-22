from storage.forecast_state_storage import ForecastStateStorage


def main() -> None:
    storage = ForecastStateStorage()

    asset = "BTC"
    state = storage.get_latest_state(asset)

    if state is None:
        print(f"No state found for asset: {asset}")
        return

    if state.current_price < state.lower_bound or state.current_price > state.upper_bound:
        state.anomaly_flag = True
        state.alert_status = "TRIGGERED"
    else:
        state.anomaly_flag = False
        state.alert_status = "OK"

    storage.put_latest_state(state)

    print(f"Anomaly check completed for asset: {asset}")
    print(f"Current price: {state.current_price}")
    print(f"Expected range: [{state.lower_bound}, {state.upper_bound}]")
    print(f"Anomaly flag: {state.anomaly_flag}")
    print(f"Alert status: {state.alert_status}")


if __name__ == "__main__":
    main()