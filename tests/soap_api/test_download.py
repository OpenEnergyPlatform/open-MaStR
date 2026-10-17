import pytest
import responses

from open_mastr.soap_api import download
from open_mastr.soap_api.download import MaStRAPI, MissingMaStRAPIKeyError


def _check_credentials_configured():
    try:
        MaStRAPI()
        return True
    except MissingMaStRAPIKeyError:
        return False


requires_real_credentials = pytest.mark.skipif(
    not _check_credentials_configured(),
    reason="MaStR credentials not configured",
)


@pytest.fixture
def mastr_api_fake_credentials():
    # MaStRAPI loads the WSDL on init, so allow it before pytest-responses blocks it
    responses.add_passthru("https://www.marktstammdatenregister.de")
    return MaStRAPI(user="testuser", key="testpassword")


@pytest.fixture
def mastr_api():
    responses.add_passthru("https://www.marktstammdatenregister.de")
    return MaStRAPI()


@pytest.mark.parametrize("token", [None, ""])
def test_missing_api_key_raises(monkeypatch, token):
    monkeypatch.setattr(download.cred, "get_mastr_user", lambda: "testuser")
    monkeypatch.setattr(download.cred, "get_mastr_token", lambda user: token)

    with pytest.raises(MissingMaStRAPIKeyError):
        MaStRAPI()


@responses.activate
@pytest.mark.dependency(name="db_reachable")
def test_soap_wrapper_connection(mastr_api_fake_credentials):
    responses.add_passthru("https://www.marktstammdatenregister.de")
    mastr_api = mastr_api_fake_credentials
    lokale_uhrzeit = mastr_api.GetLokaleUhrzeit()

    assert lokale_uhrzeit["Ergebniscode"] == "OK"


@requires_real_credentials
@responses.activate
@pytest.mark.dependency(depends=["db_reachable"])
def test_soap_wrapper_contingent(mastr_api):
    responses.add_passthru("https://www.marktstammdatenregister.de")
    contingent = mastr_api.GetAktuellerStandTageskontingent()

    assert contingent["Ergebniscode"] == "OK"


@requires_real_credentials
@responses.activate
@pytest.mark.dependency(depends=["db_reachable"])
def test_soap_wrapper_power_plant_list(mastr_api):
    responses.add_passthru("https://www.marktstammdatenregister.de")
    response = mastr_api.GetGefilterteListeStromErzeuger(limit=1)

    for key in [
        "EinheitMastrNummer",
        "DatumLetzeAktualisierung",
        "Name",
        "Einheitart",
        "Einheittyp",
        "Standort",
        "Bruttoleistung",
        "Erzeugungsleistung",
        "EinheitBetriebsstatus",
        "Anlagenbetreiber",
        "EegMastrNummer",
        "KwkMastrNummer",
        "SpeMastrNummer",
        "GenMastrNummer",
        "BestandsanlageMastrNummer",
        "NichtVorhandenInMigriertenEinheiten",
    ]:
        for einheit in response["Einheiten"]:
            assert key in einheit

    assert response["Ergebniscode"] == "OkWeitereDatenVorhanden"
