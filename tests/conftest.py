"""
The conftest.py file serves as a means of providing fixtures for an entire directory.
Fixtures defined in a conftest.py can be used by any test in that package without
needing to import them (pytest will automatically discover them).

You can have multiple nested directories/packages containing your tests,
and each directory can have its own conftest.py with its own fixtures,
adding on to the ones provided by the conftest.py files in parent directories.

https://docs.pytest.org/en/7.2.x/reference/fixtures.html
"""

import pytest
from open_mastr import Mastr
import os
import shutil


@pytest.fixture()
def db():
    """Create a sqlite testing db and remove it after usage."""
    OUTPUT_PATH = os.path.expanduser("~/.open-mastr-testing")
    if os.path.exists(OUTPUT_PATH):
        shutil.rmtree(OUTPUT_PATH)
    os.environ["OUTPUT_PATH"] = OUTPUT_PATH
    db = Mastr()
    yield db
    # Run this code after the db is used in a test.
    # This makes the testing reproducible.
    # Note: Only works for sqlite based testing.
    db.engine.dispose()
    shutil.rmtree(OUTPUT_PATH)


@pytest.fixture()
def db_translated(db):
    db.translate()

    return db
