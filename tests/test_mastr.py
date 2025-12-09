import shutil
import re
import os
import sqlalchemy
import pandas as pd
from pathlib import Path
from open_mastr.utils.constants import TRANSLATIONS
from datetime import date, timedelta


def test_Mastr_init(db):
    assert os.path.exists(db.output_dir), "home directory was not created"
    print(type(db.output_dir))
    assert os.path.exists(Path(db.output_dir) / "data"), (
        "data directory was not created"
    )
    assert os.path.exists(db._sqlite_folder_path), "sqlite directory was not created"
    assert isinstance(db.engine, sqlalchemy.engine.Engine), (
        "database engine is not a proper sqlalchemy engine"
    )


def test_mastr_download(db):
    db.download(data=["electricity_consumer"])
    # Test if database table is filled
    df_consumers = pd.read_sql("electricity_consumer", con=db.engine)
    assert len(df_consumers) > 100, "electricity consumer table is not present"

    # Test if expected columns are present
    test_columns = [
        ("EinheitMastrNummer", True),
        ("DatumLetzteAktualisierung", True),
        ("DatenQuelle", True),
        ("blorb", False),
    ]
    columns = df_consumers.columns
    for name, is_expected in test_columns:
        assert (name in columns) == is_expected, (
            f"column {name} should{' ' if is_expected else ' not'} be in the database"
        )

    # Test if old xml files are kept
    folder = Path(db.output_dir) / "data" / "xml_download"
    matches = list(folder.glob("Gesamtdatenexport_*"))
    assert matches, "no downloaded files are present"
    file_today = matches[0].name
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    file_old = re.sub(r"\d{8}", yesterday, os.path.basename(file_today))
    file_old = folder / file_old
    shutil.copy(folder / file_today, file_old)

    db.download(data="gsgk", keep_old_files=True)
    assert os.path.exists(file_old), "the old zip files do not exist"


def test_Mastr_translate_columns(db):
    db.translate()
    # test if columns got translated
    inspector = sqlalchemy.inspect(db.engine)
    table_names = inspector.get_table_names()

    for table in table_names:
        for column in inspector.get_columns(table):
            column = column["name"]
            assert (
                column in TRANSLATIONS.values() or column not in TRANSLATIONS.keys()
            ), f"{column} was not translated"
