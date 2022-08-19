import datetime
import pytest
import random
import pandas as pd
from os.path import join

import pytest
from open_mastr.soap_api.mirror import MaStRMirror
from open_mastr.utils import orm
from open_mastr.utils.config import get_project_home_dir, get_data_version_dir
from open_mastr.utils.data_io import read_csv_data
from open_mastr.utils.helpers import create_database_engine, session_scope

TECHNOLOGIES = random.sample(
    [
        "wind",
        "hydro",
        "solar",
        "biomass",
        "nuclear",
        "gsgk",
        "storage",
    ],
    k=3,
)
DATA_TYPES = ["unit_data", "eeg_data", "kwk_data", "permit_data"]
LOCATION_TYPES = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]
LIMIT = 1
DATE = datetime.datetime(2020, 11, 27, 0, 0, 0)


@pytest.fixture
def mastr_mirror():
    engine = create_database_engine("sqlite", get_project_home_dir())
    return MaStRMirror(engine=engine)


@pytest.fixture
def engine():
    return create_database_engine("sqlite", get_project_home_dir())


@pytest.mark.dependency(name="backfill_basic")
def test_backfill_basic(mastr_mirror, engine):
    mastr_mirror.backfill_basic(data=TECHNOLOGIES, date=DATE, limit=LIMIT)

    # The table basic_units should have at least as much rows as TECHNOLOGIES were queried
    with session_scope(engine=engine) as session:
        response = session.query(orm.BasicUnit).count()
        assert response >= len(TECHNOLOGIES)


@pytest.mark.dependency(depends=["backfill_basic"], name="retrieve_additional_data")
def test_retrieve_additional_data(mastr_mirror):
    for tech in TECHNOLOGIES:
        for data_type in DATA_TYPES:
            mastr_mirror.retrieve_additional_data(
                data=tech, data_type=data_type, limit=10 * LIMIT
            )

    # This comparison currently fails because of
    # https://github.com/OpenEnergyPlatform/open-MaStR/issues/154
    # with session_scope() as session:
    #     for data in TECHNOLOGIES:
    #         mapper = getattr(orm, mastr_mirror.orm_map[data]["unit_data"])
    #         response = session.query(mapper).count()
    #         assert response >= LIMIT


@pytest.mark.dependency(depends=["retrieve_additional_data"], name="update_latest")
def test_update_latest(mastr_mirror, engine):
    mastr_mirror.backfill_basic(data=TECHNOLOGIES, date="latest", limit=LIMIT)

    # Test if latest date is newer that initially requested data in backfill_basic
    with session_scope(engine=engine) as session:
        response = (
            session.query(orm.BasicUnit.DatumLetzteAktualisierung)
            .order_by(orm.BasicUnit.DatumLetzteAktualisierung.desc())
            .first()
        )
    assert response.DatumLetzteAktualisierung > DATE


@pytest.mark.dependency(
    depends=["update_latest"], name="create_additional_data_requests"
)
def test_create_additional_data_requests(mastr_mirror, engine):
    with session_scope(engine=engine) as session:
        for tech in TECHNOLOGIES:
            session.query(orm.AdditionalDataRequested).filter_by(
                technology="gsgk"
            ).delete()
            session.commit()
            mastr_mirror.create_additional_data_requests(tech, data_types=DATA_TYPES)


@pytest.mark.dependency(
    depends=["create_additional_data_requests"], name="export_to_csv"
)
def test_to_csv(mastr_mirror, engine):
    with session_scope(engine=engine) as session:
        for tech in TECHNOLOGIES:
            mastr_mirror.to_csv(
                technology=tech,
                additional_data=DATA_TYPES,
                statistic_flag=None,
                chunksize=1,
            )
            # Test if all EinheitMastrNummer in basic_units are included in CSV file
            csv_path = join(
                get_data_version_dir(),
                f"bnetza_mastr_{tech}_raw.csv",
            )
            df = pd.read_csv(csv_path, index_col="EinheitMastrNummer")
            units = session.query(orm.BasicUnit.EinheitMastrNummer).filter(
                orm.BasicUnit.Einheittyp == mastr_mirror.unit_type_map_reversed[tech]
            )
            for unit in units:
                assert unit.EinheitMastrNummer in df.index


@pytest.mark.dependency(name="backfill_locations_basic")
def test_backfill_locations_basic(mastr_mirror, engine):
    with session_scope(engine=engine) as session:
        rows_before_download = session.query(orm.LocationBasic).count()

    mastr_mirror.backfill_locations_basic(date="latest", limit=LIMIT)

    # The table locations_basic should have rows_before_download + LIMIT rows
    with session_scope(engine=engine) as session:
        rows_after_download = session.query(orm.LocationBasic).count()
        rows_downloaded = rows_after_download - rows_before_download
        # Downloaded rows might already exist, therefore less or equal
        assert rows_downloaded <= LIMIT


@pytest.mark.dependency(
    depends=["backfill_locations_basic"], name="test_retrieve_additional_location_data"
)
def test_retrieve_additional_location_data(mastr_mirror):
    """Test if code runs successfully"""
    for location_type in LOCATION_TYPES:
        mastr_mirror.retrieve_additional_location_data(location_type, limit=LIMIT)


def test_append_additional_data_from_basic_unit(mastr_mirror):

    data_list = []
    basic_unit = {
        "EinheitMastrNummer": "SEE946206606199",
        "Einheitart": "Stromerzeugungseinheit",
        "Einheittyp": "Windeinheit",
        "EegMastrNummer": "EEG993769703803",
        "KwkMastrNummer": None,
        "GenMastrNummer": "SGE924412236812",
    }

    for basic_unit_identifier, data_type in [
        ("EinheitMastrNummer", "unit_data"),
        ("EegMastrNummer", "eeg_data"),
        ("KwkMastrNummer", "kwk_data"),
        ("GenMastrNummer", "permit_data"),
    ]:
        data_list = mastr_mirror._append_additional_data_from_basic_unit(
            data_list, basic_unit, basic_unit_identifier, data_type
        )
        assert type(data_list) == list
        if data_type != "kwk_data":
            assert len(data_list) > 0
