from open_mastr.soap_api.download import MaStRAPI
import pytest


@pytest.fixture
def mastr_api_fake_credentials():
    return MaStRAPI(user="testuser", key="testpassword")


@pytest.fixture
def mastr_api():
    return MaStRAPI()


@pytest.mark.dependency(name="db_reachable")
def test_soap_wrapper_connection(mastr_api_fake_credentials):
    mastr_api = mastr_api_fake_credentials
    lokale_uhrzeit = mastr_api.GetLokaleUhrzeit()

    assert lokale_uhrzeit['Ergebniscode'] == 'OK'


@pytest.mark.dependency(depends=["db_reachable"])
def test_soap_wrapper_contingent(mastr_api):
    contingent = mastr_api.GetAktuellerStandTageskontingent()

    assert contingent['Ergebniscode'] == 'OK'


@pytest.mark.dependency(depends=["db_reachable"])
def test_soap_wrapper_power_plant_list(mastr_api):
    response = mastr_api.GetGefilterteListeStromErzeuger(limit=1)
    import pprint
    pprint.pprint(response)

    for key in [
        "EinheitMastrNummer", "DatumLetzeAktualisierung", "Name",
        "Einheitart", "Einheittyp", "Standort", "Bruttoleistung",
        "Erzeugungsleistung", "EinheitBetriebsstatus",
        "Anlagenbetreiber", "EegMastrNummer", "KwkMastrNummer",
        "SpeMastrNummer", "GenMastrNummer", "BestandsanlageMastrNummer",
        "NichtVorhandenInMigriertenEinheiten", "StatisikFlag"
    ]:
        for einheit in response["Einheiten"]:
            assert key in einheit

    assert response['Ergebniscode'] == 'OkWeitereDatenVorhanden'