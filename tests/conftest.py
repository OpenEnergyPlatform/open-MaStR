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

from open_mastr.utils.config import get_project_home_dir
from open_mastr.utils.helpers import create_database_engine
import os


@pytest.fixture(scope="function")
def make_Mastr_class():
    """
    Factory to create different Mastr class objects.

    Parameters
    ----------
    engine_type: str
        Define type of engine, for details see
        :meth: `~.open_mastr.utils.helpers.create_database_engine`

    Returns
    -------
        Mastr class object
    """

    def _make_Mastr_class(engine_type):
        return Mastr(engine=engine_type)

    return _make_Mastr_class


@pytest.fixture
def engine():
    return create_database_engine(
        "sqlite", os.path.join(get_project_home_dir(), "data", "sqlite")
    )
