from .alpha_vantage import AlphaVantageProcessor

# To add a new data source:
#   1. Create processors/your_api.py extending BaseProcessor
#   2. Import and register it here
#   3. Add a source entry in config/pipeline.json pointing to the new key

REGISTRY = {
    "alpha_vantage": AlphaVantageProcessor,
}


def get_processor(name: str) -> "BaseProcessor":
    if name not in REGISTRY:
        raise ValueError(f"Unknown processor '{name}'. Registered: {list(REGISTRY)}")
    return REGISTRY[name]()
