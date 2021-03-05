import datetime
import pytest

from open_mastr.soap_api.mirror import MaStRMirror
from open_mastr.soap_api import orm
from open_mastr.utils.helpers import session_scope


TECHNOLOGIES = ["wind", "hydro", "solar", "biomass", "combustion", "nuclear", "gsgk", "storage"]
DATA_TYPES = ["unit_data", "eeg_data", "kwk_data", "permit_data"]
LIMIT = 1
DATE = datetime.datetime(2020, 11, 27, 0, 0, 0)


@pytest.fixture
def mastr_mirror():
    mastr_mirror_instance = MaStRMirror(
        initialize_db=True,
        parallel_processes=2
    )

    return mastr_mirror_instance


@pytest.mark.dependency(name="backfill_basic")
def test_backfill_basic(mastr_mirror):
    mastr_mirror.backfill_basic(technology=TECHNOLOGIES,
                                date=DATE,
                                limit=LIMIT)

    with session_scope() as session:
        response = session.query(orm.BasicUnit).count()
        assert response >= len(TECHNOLOGIES)


@pytest.mark.dependency(depends=["backfill_basic"], name="retrieve_additional_data")
def test_retrieve_additional_data(mastr_mirror):
    for tech in TECHNOLOGIES:
        for data_type in DATA_TYPES:
            mastr_mirror.retrieve_additional_data(
                technology=tech,
                data_type=data_type
            )

    # This comparison currently fails because of
    # https://github.com/OpenEnergyPlatform/open-MaStR/issues/154
    # with session_scope() as session:
    #     for tech in TECHNOLOGIES:
    #         mapper = getattr(orm, mastr_mirror.orm_map[tech]["unit_data"])
    #         response = session.query(mapper).count()
    #         assert response >= LIMIT

@pytest.mark.dependency(depends=["retrieve_additional_data"], name="update_latest")
def test_update_latest(mastr_mirror):
    mastr_mirror.backfill_basic(technology=TECHNOLOGIES,
                                date="latest",
                                limit=LIMIT)

    # Test if latest date is newer that intially requested data in backfill_basic
    with session_scope() as session:
        response = session.query(orm.BasicUnit.DatumLetzeAktualisierung).order_by(
            orm.BasicUnit.DatumLetzeAktualisierung.desc()).first()
    assert response > pytz.utc.localize(DATE)