import shutil

from open_mastr.mastr import Mastr
import os
import re
import sqlalchemy
import pytest
from os.path import expanduser
import pandas as pd
from open_mastr.utils.constants import TRANSLATIONS
from datetime import date, timedelta

_xml_file_exists = False
_xml_folder_path = os.path.join(expanduser("~"), ".open-MaStR", "data", "xml_download")
if os.path.isdir(_xml_folder_path):
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            _xml_file_exists = True


@pytest.fixture(scope="module")
def zipped_xml_file_path():
    zipped_xml_file_path = None
    for entry in os.scandir(path=_xml_folder_path):
        if "Gesamtdatenexport" in entry.name:
            zipped_xml_file_path = os.path.join(_xml_folder_path, entry.name)

    return zipped_xml_file_path


@pytest.fixture
def db_path():
    return os.path.join(
        os.path.expanduser("~"), ".open-MaStR", "data", "sqlite", "mastr-test.db"
    )


@pytest.fixture
def db(db_path):
    return Mastr(engine=sqlalchemy.create_engine(f"sqlite:///{db_path}"))


@pytest.fixture
def db_translated(db_path):
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    db_api = Mastr(engine=engine)

    db_api.download(date="existing", data=["wind", "hydro", "biomass", "combustion"])
    db_api.translate()

    return db_api


def test_Mastr_init(db):
    # test if folder structure exists
    assert os.path.exists(db.home_directory)
    assert os.path.exists(db._sqlite_folder_path)

    # test if engine and connection were created
    assert type(db.engine) == sqlalchemy.engine.Engine


@pytest.mark.skipif(
    not _xml_file_exists, reason="The zipped xml file could not be found."
)
def test_Mastr_translate(db_translated, db_path):
    # test if database was renamed correctly
    transl_path = db_path[:-3] + "-translated.db"
    assert os.path.exists(transl_path)

    # test if columns got translated
    inspector = sqlalchemy.inspect(db_translated.engine)
    table_names = inspector.get_table_names()

    for table in table_names:
        for column in inspector.get_columns(table):
            column = column["name"]
            assert column in TRANSLATIONS.values() or column not in TRANSLATIONS.keys()

    # test if new translated version replaces previous one
    db_translated.engine.dispose()
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    db_empty = Mastr(engine=engine)
    db_empty.translate()

    for table in table_names:
        assert pd.read_sql(sql=table, con=db_empty.engine).shape[0] == 0


@pytest.mark.dependency(name="bulk_downloaded")
def test_mastr_download(db):
    db.download(data="wind")
    df_wind = pd.read_sql("wind_extended", con=db.engine)
    assert len(df_wind) > 10000

    db.download(data="biomass")
    df_biomass = pd.read_sql("biomass_extended", con=db.engine)
    assert len(df_wind) > 10000
    assert len(df_biomass) > 10000


@pytest.mark.dependency(depends=["bulk_downloaded"])
def test_mastr_download_keep_old_files(db, zipped_xml_file_path):
    file_today = zipped_xml_file_path
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    file_old = re.sub(r"\d{8}", yesterday, os.path.basename(file_today))
    file_old = os.path.join(os.path.dirname(zipped_xml_file_path), file_old)
    shutil.copy(file_today, file_old)
    db.download(data="gsgk", keep_old_files=True)

    assert os.path.exists(file_old)
