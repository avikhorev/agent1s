import os
import pytest

def pytest_collection_modifyitems(config, items):
    if not os.getenv("ODATA_MOCK_URL"):
        skip = pytest.mark.skip(reason="ODATA_MOCK_URL not set")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
