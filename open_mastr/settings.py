import os
from os.path import expanduser

# TODO: Migrate the DB settings from the bulk download
DB_ENGINE = os.environ.get("DB_ENGINE", "sqlite")

SQLITE_DATABASE_PATH = os.environ.get("SQLITE_DATABASE_PATH", os.path.join(
            expanduser("~"), ".open-MaStR", "data", "sqlite", "open-mastr.db"
        ))

DB_URL = (
    "postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr"
    if DB_ENGINE == "docker" else f"sqlite:///{SQLITE_DATABASE_PATH}"
)
