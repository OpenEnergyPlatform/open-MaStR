from open_mastr.utils.config import setup_project_home

setup_project_home()

# This import should be called after the setup project home, since .open_mastr folder must be defined first
from .mastr import Mastr  # noqa: E402 ignore import order warning of flake8
