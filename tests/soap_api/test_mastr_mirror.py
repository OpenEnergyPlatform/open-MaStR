import datetime
import pytest

from open_mastr.soap_api.mirror import MaStRMirror


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
