from open_mastr.soap_api.download import MaStRAPI, MaStRDownload
import pytest


@pytest.fixture
def mastr_api_fake_credentials():
    return MaStRAPI(user="testuser", key="testpassword")


@pytest.fixture
def mastr_api():
    return MaStRAPI()


@pytest.fixture
def mastr_download():
    return MaStRDownload()


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


def test_basic_unit_data(mastr_download):
    data = [unit for sublist in mastr_download.basic_unit_data(
        technology="nuclear",
        limit=1
    ) for unit in sublist]

    assert len(data) == 1
    assert data[0]["Einheittyp"] == "Kernenergie"


def test_additional_data_nuclear(mastr_download):

    data_fcns = [
        ("SME963513379837", "extended_unit_data"),
        ("SGE951929415553", "permit_unit_data")]

    for mastr_nummer, data_fcn in data_fcns:
        units_downloaded, units_missed = mastr_download.additional_unit_data("nuclear", [mastr_nummer], data_fcn)

        assert len(units_downloaded) + len(units_missed) == 1


def test_additional_data_biomass(mastr_download):

    data_fcns = [
        ("SEE936595511945", "extended_unit_data"),
        ("EEG929630520224", "extended_unit_data"),
        ("KWK998480117397", "kwk_unit_data"),
        ("SGE974254715891", "permit_unit_data")]

    for mastr_nummer, data_fcn in data_fcns:
        units_downloaded, units_missed = mastr_download.additional_unit_data("biomass", [mastr_nummer], data_fcn)

        assert len(units_downloaded) + len(units_missed) == 1

