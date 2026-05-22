from open_mastr.soap_api.download import MaStRAPI
import pytest
import responses


def _check_real_credentials():
    try:
        api = MaStRAPI()
        result = api.GetAktuellerStandTageskontingent()
        return result.get("Ergebniscode") == "OK"
    except Exception:
        return False


requires_real_credentials = pytest.mark.skipif(
    not _check_real_credentials(),
    reason="Real MaStR credentials not available or not working",
)


@pytest.fixture
def mastr_api_fake_credentials():
    return MaStRAPI(user="testuser", key="testpassword")


@pytest.fixture
def mastr_api():
    return MaStRAPI()


@requires_real_credentials
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
