"""
tests/conftest.py
──────────────────
Shared pytest configuration and fixtures.
"""



# Tell pytest-asyncio to use asyncio mode for all tests in this package
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    )