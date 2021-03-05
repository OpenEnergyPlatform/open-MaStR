import datetime
import pytest

from open_mastr.soap_api.mirror import MaStRMirror
from open_mastr.soap_api.orm import BasicUnit
from open_mastr.utils.helpers import session_scope


TECHNOLOGIES = ["wind", "hydro", "solar", "biomass", "combustion", "nuclear", "gsgk", "storage"]


@pytest.fixture
def mastr_mirror():
    mastr_mirror_instance = MaStRMirror(
        initialize_db=True,
        parallel_processes=2
    )

    return mastr_mirror_instance


def test_backfill_basic(mastr_mirror):
    mastr_mirror.backfill_basic(technology=TECHNOLOGIES,
                                date=datetime.datetime(2020, 11, 27),
                                limit=1)

    with session_scope() as session:
        response = session.query(BasicUnit).count()
        assert response >= len(TECHNOLOGIES)


