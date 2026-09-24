from open_mastr.utils.config import setup_project_home

setup_project_home()

# This import should be called after the setup project home,
# since .open_mastr folder must be defined first
from .mastr import Mastr  # noqa: E402 import must follow setup_project_home()
from .utils.sqlalchemy_tables import format_mastr_table_to_db_table  # noqa: E402

__all__ = ["Mastr", "format_mastr_table_to_db_table"]
