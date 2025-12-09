"""
The conftest.py file provides fixtures for the test suite, ensuring that each
test gets an isolated, temporary SQLite database. Using pytest's built‑in
``tmp_path_factory`` guarantees a unique directory per test session (or per
test when desired) and avoids side‑effects caused by hard‑coded locations such
as ``~/.open-mastr-testing``. This approach also safely restores any existing
``OUTPUT_PATH`` environment variable after the test completes.
"""

import os
import shutil

import pytest
from open_mastr import Mastr


@pytest.fixture()
def db(tmp_path_factory):
    """
    Create a temporary SQLite testing database and clean it up after the test.

    The fixture:
    1. Creates a unique temporary directory via ``tmp_path_factory``.
    2. Instantiates ``Mastr`` (which will use the temporary path).
    3. Yields the ``Mastr`` instance to the test.
    4. After the test, disposes the engine, restores the original environment
       variable, and removes the temporary directory.
    """

    output_path = tmp_path_factory.mktemp("open-mastr-testing")
    os.environ["OUTPUT_PATH"] = str(output_path)
    db = Mastr()

    try:
        yield db
    finally:
        os.environ.pop("OUTPUT_PATH", None)
        shutil.rmtree(str(output_path), ignore_errors=True)
