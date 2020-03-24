## MaStR Processing

The description of each data version and the version of the code with which it was downloaded is available in the
[data-release-notes.md](https://github.com/OpenEnergyPlatform/open-MaStR/blob/master/soap_api/data-release-notes.md)

### Links

* [GitHub](https://github.com/OpenEnergyPlatform/data-preprocessing/issues/13)
* [API doc build](https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/2019-01_31%20Funktionen%20MaStR%20Webdienste%20V1.2.html)

### Glossary

| German  | Code | English  | Variable  |
|---|---|---|---|
| Stromnetzbetreiber | SNB  | electricity grid operator  |   |
| Einheit |   | unit  | unit  |
| Lokation | SEL  | location  | location  |
| Stromerzeugungseinheit  | SEE  | power generation unit  | power_unit  |
| Solareinheit  |   | solar  | unit_solar  |
| Wassereinheit  |   | hydro  | unit_hydro  |
| Windeinheit  |   | wind  | unit_wind  |
| Biomasseeinheit  |   | biomass  | unit_biomass  |
| Verbrennungseinheit  |   | combustion unit  | unit_combustion  |
| Einheit Geothermie, Grubengas, Solarthermie, Klärschlamm  | GGSK  |   | unit_ggsk  |
| Kernenergieeinheit  |   | nuclear  | unit_nuclear  |
| Stromspeichereinheit  | SSE  | energy storage  | unit_storage  |
|   |   |   |   |
| EEG-Anlage  | EEG  |   | _eeg  |
| KWK-Anlagen  | KWK  |   | _kwk  |
|   |   |   |   |
| Strom - Netzanschlusspunkt  | SNA  |   | grid_connection_power  |
| Gas - Netzanschlusspunkt  | GNA  |   | grid_connection_gas  |
|   |   |   |   |
| Genehmigung |   | permit | _permit |

### Variables

| German | Variable | Table |
|---|---|---|
| Stromerzeugungseinheit  | power_unit  | bnetza_mastr_{v}_power-unit.csv |
|   |   |  |
| Stromerzeugungseinheit-Wind | power_unit_wind  | bnetza_mastr_{v}_power-unit-wind.csv |
| Windeinheit | unit_wind | bnetza_mastr_{v}_unit-wind.csv |
| EEG-Anlage-Wind | unit_wind_eeg  | bnetza_mastr_{v}_unit-wind-eeg.csv |
| Genehmigung Windeinheit | unit_wind_permit | bnetza_mastr_{v}_unit-wind-permit.csv |
|   |   |  |
| Stromerzeugungseinheit-Wasser | power_unit_hydro | bnetza_mastr_{v}_power-unit-hydro.csv |
| Wassereinheit | unit_hydro | bnetza_mastr_{v}_unit-hydro.csv |
| EEG-Anlage-Wasser | unit_hydro_eeg  | bnetza_mastr_{v}_unit-hydro-eeg.csv |
|   |   |  |
| Stromerzeugungseinheit-Biomasse | power_unit_biomass | bnetza_mastr_{v}_power-unit-biomass.csv |
| Biomasseeinheit | unit_biomass | bnetza_mastr_{v}_unit-biomass.csv |
| EEG-Anlage-Biomasse | unit_biomass_eeg  | bnetza_mastr_{v}_unit-biomass-eeg.csv |
|   |   |  |
| Stromerzeugungseinheit-Solar | power_unit_solar | bnetza_mastr_{v}_power-unit-solar.csv |
| Solareinheit | unit_solar | bnetza_mastr_{v}_unit-solar.csv |
| EEG-Anlage-Solar | unit_solar_eeg  | bnetza_mastr_{v}_unit-solar-eeg.csv |
|   |   |  |
| EinheitMastrNummer  | mastr_unit_id  |  |
| EegMastrNummer  | mastr_unit_eeg_id  |  |
| GenMastrNummer  | mastr_unit_permit_id  |  |
|   |   |  |
| AktuellesLimitTageskontingent  | daily_contingent_limit  |  |
| AktuellerStandTageskontingent  | daily_contingent_status  |  |



### Getting started

- Create a virtualenv
- Run python setup.py install
- Optional: Read the Documentation to setup _download_parallel_power_unit_ 
- Set parameters in _soap_api/utils.py_
    - _DATA_VERSION_
    - _TOTAL_POWER_UNITS_ - Query from [Website](https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/ErweiterteOeffentlicheEinheitenuebersicht)
- Decide which technologies to download in _main.py_
- Run python soap_api/main.py 
