from storage.forecast_state_storage import ForecastStateStorage


def main() -> None:
    storage = ForecastStateStorage() # creation of the object and binding with the dynamodb table in aws

    asset = "BTC"
    state = storage.get_latest_state(asset) # it calls the get from the new object created

    if state is None:
        print(f"No state found for asset: {asset}")
        return

    print("Retrieved state:")
    print(state.model_dump_json(indent=2))


if __name__ == "__main__":
    main()