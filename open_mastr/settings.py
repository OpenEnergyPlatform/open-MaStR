import os

DB_ENGINE = os.environ.get("DB_ENGINE", "sqlite")
DB_URL = (
    "postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr"
    if DB_ENGINE == "docker" else "sqlite:///master.db"
)
