from open_mastr.soap_api.download import MaStRAPI, MaStRDownload, flatten_dict
import pytest
import datetime


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

    assert lokale_uhrzeit["Ergebniscode"] == "OK"


@pytest.mark.dependency(depends=["db_reachable"])
def test_soap_wrapper_contingent(mastr_api):
    contingent = mastr_api.GetAktuellerStandTageskontingent()

    assert contingent["Ergebniscode"] == "OK"


@pytest.mark.dependency(depends=["db_reachable"])
def test_soap_wrapper_power_plant_list(mastr_api):
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


def test_basic_unit_data(mastr_download):
    data = [
        unit
        for sublist in mastr_download.basic_unit_data(data="nuclear", limit=1)
        for unit in sublist
    ]

    assert len(data) == 1
    assert data[0]["Einheittyp"] == "Kernenergie"


def test_additional_data_nuclear(mastr_download):

    data_fcns = [
        ("SME963513379837", "extended_unit_data"),
        ("SGE951929415553", "permit_unit_data"),
    ]

    for mastr_nummer, data_fcn in data_fcns:
        units_downloaded, units_missed = mastr_download.additional_data(
            "nuclear", [mastr_nummer], data_fcn
        )

        assert len(units_downloaded) + len(units_missed) == 1


def test_additional_data_biomass(mastr_download):

    data_fcns = [
        ("SEE936595511945", "extended_unit_data"),
        ("EEG929630520224", "extended_unit_data"),
        ("KWK998480117397", "kwk_unit_data"),
        ("SGE974254715891", "permit_unit_data"),
    ]

    for mastr_nummer, data_fcn in data_fcns:
        units_downloaded, units_missed = mastr_download.additional_data(
            "biomass", [mastr_nummer], data_fcn
        )

        assert len(units_downloaded) + len(units_missed) == 1


def test_flatten_dict():
    data_before_flatten = [
        {
            "Frist": {"Wert": datetime.date(2017, 6, 15), "NichtVorhanden": False},
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": {"Wert": None, "NichtVorhanden": False},
            "Registrierungsdatum": datetime.date(2019, 9, 11),
            "VerknuepfteEinheiten": [
                {
                    "MaStRNummer": "SEE900290686291",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                }
            ],
        },
        {
            "Frist": {"Wert": datetime.date(2018, 3, 27), "NichtVorhanden": False},
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": {"Wert": None, "NichtVorhanden": False},
            "Registrierungsdatum": datetime.date(2019, 2, 4),
            "VerknuepfteEinheiten": [
                {
                    "MaStRNummer": "SEE908999761141",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
                {
                    "MaStRNummer": "SEE978389475514",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
                {
                    "MaStRNummer": "SEE960287756734",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
            ],
        },
        {
            "Frist": {"Wert": datetime.date(2018, 3, 23), "NichtVorhanden": False},
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": {"Wert": None, "NichtVorhanden": False},
            "Registrierungsdatum": datetime.date(2019, 2, 15),
            "VerknuepfteEinheiten": [
                {
                    "MaStRNummer": "SEE902566900605",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
                {
                    "MaStRNummer": "SEE945851284479",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
                {
                    "MaStRNummer": "SEE975973981666",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
            ],
        },
        {
            "Frist": {"Wert": datetime.date(2016, 1, 23), "NichtVorhanden": False},
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": {"Wert": None, "NichtVorhanden": False},
            "Registrierungsdatum": datetime.date(2019, 2, 7),
            "VerknuepfteEinheiten": [
                {
                    "MaStRNummer": "SEE969499157391",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
                {
                    "MaStRNummer": "SEE949984955732",
                    "Einheittyp": "Windeinheit",
                    "Einheitart": "Stromerzeugungseinheit",
                },
            ],
        },
    ]
    data_after_flatten = [
        {
            "Frist": datetime.date(2017, 6, 15),
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": None,
            "Registrierungsdatum": datetime.date(2019, 9, 11),
            "VerknuepfteEinheiten": "SEE900290686291",
        },
        {
            "Frist": datetime.date(2018, 3, 27),
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": None,
            "Registrierungsdatum": datetime.date(2019, 2, 4),
            "VerknuepfteEinheiten": "SEE908999761141, SEE978389475514, SEE960287756734",
        },
        {
            "Frist": datetime.date(2018, 3, 23),
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": None,
            "Registrierungsdatum": datetime.date(2019, 2, 15),
            "VerknuepfteEinheiten": "SEE902566900605, SEE945851284479, SEE975973981666",
        },
        {
            "Frist": datetime.date(2016, 1, 23),
            "WasserrechtsNummer": None,
            "WasserrechtAblaufdatum": None,
            "Registrierungsdatum": datetime.date(2019, 2, 7),
            "VerknuepfteEinheiten": "SEE969499157391, SEE949984955732",
        },
    ]
    assert flatten_dict(data_before_flatten) == data_after_flatten
