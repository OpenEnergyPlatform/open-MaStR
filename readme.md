## MaStR Processing

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

### Variables

| German | Variable | Table |
|---|---|---|
| Stromerzeugungseinheit  | power_unit  | bnetza_mastr_{v}_power-unit.csv |
|   |   |  |
| Stromerzeugungseinheit-Wind | power_unit_wind  | bnetza_mastr_{v}_power-unit-wind.csv |
| Windeinheit | unit_wind | bnetza_mastr_{v}_unit-wind.csv |
| EEG-Anlage-Wind | unit_wind_eeg  | bnetza_mastr_{v}_unit-wind-eeg.csv |
|   |   |  |
| Stromerzeugungseinheit-Wasser | power_unit_hydro | bnetza_mastr_{v}_power-unit-hydro.csv |
| Wassereinheit | unit_hydro | bnetza_mastr_{v}_unit-hydro.csv |
| EEG-Anlage-Wasser | unit_hydro_eeg  | bnetza_mastr_{v}_unit-hydro-eeg.csv |
|   |   |  |
|   |   |  |
|   |   |  |
|   |   |  |
| EinheitMastrNummerId  | mastr_unit  |  |
| EegMastrNummerId  | mastr_unit_eeg  |  |