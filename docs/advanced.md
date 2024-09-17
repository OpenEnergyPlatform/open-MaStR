For most users, the functionalites described in [Getting Started](getting_started.md) are sufficient.  If you want 
to examine how you can configure the package's behavior for your own needs, check out [Configuration](#configuration). Or you can explore the two main functionalities of the package, namely the [Bulk Download](#bulk-download)
or the [SOAP API download](#soap-api-download).

## Configuration
### Database settings


Configure your database with the `engine` parameter of [`Mastr`][open_mastr.Mastr].
It defines the engine of the database where the MaStR is mirrored to. Default is 'sqlite'.

The possible databases are:

* **sqlite**: By default the database will be stored in `$HOME/.open-MaStR/data/sqlite/open-mastr.db`.
* **own database**: The Mastr class accepts a sqlalchemy.engine.Engine object as engine which enables the user to
  use any other desired database.
  If you do so, you need to insert the connection parameter into the engine variable. It'll look like this:

```python

  from sqlalchemy import create_engine

  engine_postgres = create_engine("postgresql+psycopg2://open-mastr:open-mastr@localhost:55443/open-mastr")
  engine_sqlite = create_engine("sqlite:///path/to/sqlite/database.db")
  db = Mastr(engine=engine_sqlite)
```

### Project directory

The directory `$HOME/.open-MaStR` is automatically created. It is used to store configuration files and save data.
You can change this default path, see [environment variables](#environment-variables).
Default config files are copied to this directory which can be modified - but with caution.
The project home directory is structured as follows (files and folders below `data/` just an example).

```bash

    .open-MaStR/
    ├── config
    │   ├── credentials.cfg
    │   ├── filenames.yml
    │   ├── logging.yml
    ├── data
    │   ├── dataversion-<date>
    │   ├── sqlite
    │       └── open-mastr.db
        └── xml_download
            └── Gesamtdatenexport_<date>.zip
    └── logs
        └── open_mastr.log
```
 
 
* **config**
     * `credentials.cfg` <br>
        Credentials used to access
        [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) API
     * `filenames.yml` <br>
        File names are defined here.
     * `logging.yml` <br>
        Logging configuration. For changing the log level to increase or decrease details of log
        messages, edit the level of the handlers.
* **data**
     * `dataversion-<date>` <br>
        Contains exported data as csv files from method [`to_csv`][open_mastr.Mastr.to_csv]
     * `sqlite` <br>
        Contains the sqlite database in `open-mastr.db`
     * `xml_download` <br>
        Contains the bulk download in `Gesamtdatenexport_<date>.zip` <br>
        New bulk download versions overwrite older versions. 
* **logs**
     *  `open_mastr.log` <br>
        The files stores the logging information from executing open-mastr.



### Logs

For the download via the API, logs are stored in a single file in `/$HOME/<user>/.open-MaStR/logs/open_mastr.log`.
New logging messages are appended. It is recommended to delete the log file from time to time because of its required disk space.


### Data

If the zipped dump of the MaStR is downloaded, it is saved in the folder `$HOME/.open-MaStR/data/xml_download`. 

The data can then be written to any sql database supported by [sqlalchemy](https://docs.sqlalchemy.org/). The type of the sql database is determined by the parameter `engine` in the [Mastr][open_mastr.Mastr] class.

For more information regarding the database see [Database settings](#database-settings).


### Environment variables

There are some environment variables to customize open-MaStR:

| Variable               | Description                                                                                                                                                              | Example                                                                                                                    |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `SQLITE_DATABASE_PATH` | Path to the SQLite file. This allows to use to use multiple instances of the MaStR database. The database instances exist in parallel and are independent of each other. | `/home/mastr-rabbit/.open-MaStR/data/sqlite/your_custom_instance_name.db`                                                  |
| `OUTPUT_PATH`          | Path to user-defined output directory for CSV data, XML file and database. If not specified, output directory defaults to `$HOME/.open-MaStR/`                           | Linux: `/home/mastr-rabbit/open-mastr-user-defined-output-path`, Windows: `C:\\Users\\open-mastr-user-defined-output-path` |

## Bulk download

On the homepage [MaStR/Datendownload](https://www.marktstammdatenregister.de/MaStR/Datendownload) a zipped folder containing the whole
MaStR is offered. The data is delivered as xml-files. The official documentation can be found 
on the same page (in german). This data is updated on a daily base. 

``` mermaid
flowchart LR
  id1("📥 Download raw data
  from marktstammdatenregister.de") --> id2(Write xml files to database)
  id2 --> id3[("📗 open-mastr database")]
  id3 --> id4("🔧 Decode and cleanse data")
  id4 --> id3
  id3 --> id5("Merge corresponding tables
  and save as csv")
  id5 --> id6>"📜 open-mastr csv files"]
  click id1 "https://www.marktstammdatenregister.de/MaStR/Datendownload" _blank
  click id2 "https://github.com/OpenEnergyPlatform/open-MaStR/blob/7b155a9ebdd5204de8ae6ba7a96036775a1f4aec/open_mastr/xml_download/utils_write_to_database.py#L17C6-L17C6" _blank
  click id4 "https://github.com/OpenEnergyPlatform/open-MaStR/blob/7b155a9ebdd5204de8ae6ba7a96036775a1f4aec/open_mastr/xml_download/utils_cleansing_bulk.py#L10" _blank
  click id5 "https://github.com/OpenEnergyPlatform/open-MaStR/blob/7b155a9ebdd5204de8ae6ba7a96036775a1f4aec/open_mastr/mastr.py#L288" _blank
  click id6 "https://doi.org/10.5281/zenodo.6807425" _blank
```


In the following, the process is described that is started when calling the [`Mastr.download`][open_mastr.Mastr.download] function with the parameter `method`="bulk". 
First, the zipped files are downloaded and saved in `$HOME/.open-MaStR/data/xml_download`. The zipped folder contains many xml files,
which represent the different tables from the MaStR. Those tables are then parsed to a sqlite database. If only some specific
tables are of interest, they can be specified with the parameter `data`. Every table that is selected in `data` will be deleted from the local database, if existent, and then filled with data from the xml files.

In the next step, a basic data cleansing is performed. Many entries in the MaStR from the bulk download are replaced by numbers.
As an example, instead of writing the german states where the unit is registered (Saxony, Brandenburg, Bavaria, ...) the MaStR states 
corresponding digits (7, 2, 9, ...). One major step of cleansing is therefore to replace those digits with their original meaning. 
Moreover, the datatypes of different entries are set in the data cleansing process and corrupted files are repaired.

If needed, the tables in the database can be obtained as csv files. Those files are created by first merging corresponding tables (e.g all tables that contain information about solar) and then dumping those tables to `.csv` files with the [`to_csv`][open_mastr.Mastr.to_csv] method.

=== "Advantages"
    * No registration for an API key is needed
    * Download of the whole dataset is possible

=== "Disadvantages"
    * No single tables or entries can be downloaded
    * Download takes long time


## SOAP API download

### MaStR account and credentials


For downloading data from the
[Marktstammdatenregister (MaStR) database](https://www.marktstammdatenregister.de/MaStR)
via its API a [registration](https://www.marktstammdatenregister.de/MaStRHilfe/files/regHilfen/Handbuch_fuer_Registrierungen_durch_Dienstleister.pdf) is mandatory.

To download data from the MaStR API using the `open-MaStR`, the credentials (MaStR user and token) need to be provided in a certain way. Three options exist:

1. **Credentials file:** 
    Both, user and token, are stored in plain text in the credentials file.
    For storing the credentials in the credentials file (plus optionally using keyring for the token) simply instantiate
    [`MaStRDownload`][open_mastr.soap_api.download.MaStRDownload] once and you get asked for a user name and a token. The
    information you insert will be used to create the credentials file.

    It is also possible to create the credentials file by hand, using this format:

    ```
        [MaStR]
        user = SOM123456789012
        token = msöiöo8u2o29933n31733m§=§1n33§304n... # optional, 540 characters
    ```

    The `token` should be written in one line, without line breaks.

    The credentials file needs to be stored at: `$HOME/.open-MaStR/config/credentials.cfg`

2. **Credentials file + keyring:** 
    The user is stored in the credentials file, while the token is stored encrypted in the [keyring](https://pypi.org/project/keyring/).

    Read in the documentation of the keyring library how to store your token in the
    keyring.

3. **Don't store:** 
    Just use the password for one query and forget it

    The latter option is only available when using [`MaStRAPI`][open_mastr.soap_api.download.MaStRAPI].
    Instantiate with

    ```python
    MaStRAPI(user='USERNAME', key='TOKEN')
    ```

    to provide user and token in a script and use these
    credentials in subsequent queries.

### MaStRAPI

You can access the MaStR data via API by using the class `MaStRAPI` directly if you have the API credentials 
configured correctly. Use the code snippet below for queries.


```python
from open_mastr.soap_api.download import MaStRAPI

if __name__ == "__main__":

    mastr_api = MaStRAPI()
    print(mastr_api.GetLokaleUhrzeit())
```

The MaStR API has different models to query from, the default are power units
("Anlage"). To change this, you can pass the desired model to
[`MaStRAPI`][open_mastr.soap_api.download.MaStRAPI].
E.g. to query market actors instantiate it using
`MaStRAPI(service_port="Akteur")`.

For API calls, models and optional parameters refer to the
[API documentation](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html).

???+ example "Example queries and their responses (for model 'Anlage')"

    === "mastr_api.GetLokaleUhrzeit()"
        
        Response: 
        ```python
        {
        'Ergebniscode': 'OK',
        'AufrufVeraltet': False,
        'AufrufLebenszeitEnde': None,
        'AufrufVersion': 1,
        'LokaleUhrzeit': datetime.datetime(2023, 9, 25, 8, 1, 46, 24766, tzinfo=<FixedOffset '+02:00'>)
        }
        ```

        API function name: `GetLokaleUhrzeit` <br>
        Example query: `mastr_api.GetLokaleUhrzeit()` <br>
        Parameter: `None`
        

    
    === "mastr_api.GetListeAlleEinheiten(limit=1)"
        
        Response:  
        ```python
        {
        "Ergebniscode": "OkWeitereDatenVorhanden",
        "AufrufVeraltet": False,
        "AufrufLebenszeitEnde": None,
        "AufrufVersion": 1,
        "Einheiten": [
            {
                "EinheitMastrNummer": "SEE984033548619",
                "DatumLetzeAktualisierung": datetime.datetime(
                    2020, 2, 20, 16, 28, 35, 250812
                ),
                "Name": "Photovoltaikanlage ERWin4",
                "Einheitart": "Stromerzeugungseinheit",
                "Einheittyp": "Solareinheit",
                "Standort": "48147 Münster",
                "Bruttoleistung": Decimal("3.960"),
                "Erzeugungsleistung": None,
                "EinheitSystemstatus": "Aktiv",
                "EinheitBetriebsstatus": "InBetrieb",
                "Anlagenbetreiber": "ABR949444220202",
                "EegMastrNummer": "EEG920083771065",
                "KwkMastrNummer": None,
                "SpeMastrNummer": None,
                "GenMastrNummer": None,
                "BestandsanlageMastrNummer": None,
                "NichtVorhandenInMigriertenEinheiten": None,
            }
        ],
        }
        ```

        API function name: `GetEinheitSolar` <br>
        Example query: `mastr_api.GetListeAlleEinheiten(limit=1)`
    
        | Parameter                 | Description                                                              |
        |------------------------|-----------------------------------------------------------------------------------------------------------|
        | marktakteurMastrNummer | The MaStR number of the requested unit                                                                    |
        | startAb                | Sets the beginning of the records to be delivered [default value: 1]                                      |
        | datumAb                | Restrict the amount of data to be retrieved to changed data from the specified date [Default value: NULL] |
        | limit                  | Limit of the maximum data records to be delivered [default/maximum value: maximum of own limit]           |
        | einheitMastrNummern[]  |                                                                                                           |
    
    === "mastr_api.GetEinheitSolar(einheitMastrNummer="SEE984033548619")"
        
        Response: 
        ```python
        {
        "Ergebniscode": "OK",
        "AufrufVeraltet": False,
        "AufrufLebenszeitEnde": None,
        "AufrufVersion": 1,
        "EinheitMastrNummer": "SEE984033548619",
        "DatumLetzteAktualisierung": datetime.datetime(2020, 2, 20, 16, 28, 35, 250812),
        "LokationMastrNummer": "SEL948991715391",
        "NetzbetreiberpruefungStatus": "Geprueft",
        "Netzbetreiberzuordnungen": [
            {
                "NetzbetreiberMastrNummer": "SNB980883363112",
                "NetzbetreiberpruefungsDatum": datetime.date(2020, 2, 25),
                "NetzbetreiberpruefungsStatus": "Geprueft",
            }
        ],
        "NetzbetreiberpruefungDatum": datetime.date(2020, 2, 25),
        "AnlagenbetreiberMastrNummer": "ABR949444220202",
        "NetzbetreiberMastrNummer": ["SNB980883363112"],
        "Land": "Deutschland",
        "Bundesland": "NordrheinWestfalen",
        "Landkreis": "Münster",
        "Gemeinde": "Münster",
        "Gemeindeschluessel": "05515000",
        "Postleitzahl": "48147",
        "Gemarkung": None,
        "FlurFlurstuecknummern": None,
        "Strasse": None,
        "StrasseNichtGefunden": False,
        "Hausnummer": {"Wert": None, "NichtVorhanden": False},
        "HausnummerNichtGefunden": False,
        "Adresszusatz": None,
        "Ort": "Münster",
        "Laengengrad": None,
        "Breitengrad": None,
        "UtmZonenwert": None,
        "UtmEast": None,
        "UtmNorth": None,
        "GaussKruegerHoch": None,
        "GaussKruegerRechts": None,
        "Registrierungsdatum": datetime.date(2019, 2, 1),
        "GeplantesInbetriebnahmedatum": None,
        "Inbetriebnahmedatum": datetime.date(2007, 7, 20),
        "DatumEndgueltigeStilllegung": None,
        "DatumBeginnVoruebergehendeStilllegung": None,
        "DatumWiederaufnahmeBetrieb": None,
        "EinheitSystemstatus": "Aktiv",
        "EinheitBetriebsstatus": "InBetrieb",
        "BestandsanlageMastrNummer": None,
        "NichtVorhandenInMigriertenEinheiten": None,
        "AltAnlagenbetreiberMastrNummer": None,
        "DatumDesBetreiberwechsels": None,
        "DatumRegistrierungDesBetreiberwechsels": None,
        "NameStromerzeugungseinheit": "Photovoltaikanlage ERWin4",
        "Weic": {"Wert": None, "NichtVorhanden": False},
        "WeicDisplayName": None,
        "Kraftwerksnummer": {"Wert": None, "NichtVorhanden": False},
        "Energietraeger": "SolareStrahlungsenergie",
        "Bruttoleistung": Decimal("3.960"),
        "Nettonennleistung": Decimal("3.960"),
        "Schwarzstartfaehigkeit": None,
        "Inselbetriebsfaehigkeit": None,
        "Einsatzverantwortlicher": None,
        "FernsteuerbarkeitNb": False,
        "FernsteuerbarkeitDv": None,
        "FernsteuerbarkeitDr": None,
        "Einspeisungsart": "Volleinspeisung",
        "PraequalifiziertFuerRegelenergie": None,
        "GenMastrNummer": None,
        "zugeordneteWirkleistungWechselrichter": Decimal("4.000"),
        "GemeinsamerWechselrichterMitSpeicher": "KeinStromspeicherVorhanden",
        "AnzahlModule": 22,
        "Lage": "BaulicheAnlagen",
        "Leistungsbegrenzung": "Nein",
        "EinheitlicheAusrichtungUndNeigungswinkel": True,
        "Hauptausrichtung": "Sued",
        "HauptausrichtungNeigungswinkel": "Grad20Bis40",
        "Nebenausrichtung": "None",
        "NebenausrichtungNeigungswinkel": "None",
        "InAnspruchGenommeneFlaeche": None,
        "ArtDerFlaeche": [],
        "InAnspruchGenommeneAckerflaeche": None,
        "Nutzungsbereich": "Haushalt",
        "Buergerenergie": None,
        "EegMastrNummer": "EEG920083771065",
        }
        ```

        API function name: `GetEinheitSolar` <br>
        Example query: `mastr_api.GetEinheitSolar(einheitMastrNummer="SEE984033548619")` <br>

        | Parameter                | Description                                                       |
        |--------------------------|-------------------------------------------------------------------|
        | `apiKey`                 | The web service key for validation                                |
        | `marktakteurMastrNummer` | The MaStR number of the market actor used by the web service user |
        | `einheitMastrNummer`     | The MaStR number of the requested unit                            | 


??? note "Why can't I just query all information of all units of a specific power plant type?"

    As the example queries above demonstrate, the API is structured so that units of power plants types (e.g. wind 
    turbine, solar PV systems, gas power plant) have to be queried directly by their unique identifier ( 
    `EinheitMastrNummer"`) and a distinct API query. To download all unit information of a specific power plant 
    you need to know the "EinheitMastrNummer". <br>

    Firstly, by querying for all units with `mastr_api.GetListeAlleEinheiten()` you'll get all units, their unique 
    identifier (`EinheitMastrNummer`) and their power plant type (`Einheitentyp`). You can then sort them by power 
    plant type and use the power plant type specific API query to retrieve information about it. <br>

    Cumbersome? <br>
    Luckily, `open-MaStR` has you covered and provides methods to just query for all units of a power 
    plant type.    




### MaStRDownload

The class `MaStRDownload` builds upon methods provided in the class `MaStRAPI`. <br> 

It provides methods to download power plant unit types and additional information 
for each unit type, such as extended unit data, permit data, chp-specific data, location data 
or eeg-specific data. <br>

The class handles the querying logic and knows which additional data for each unit type is available 
and which SOAP service has to be used to query it. 


### MaStRMirror

The class `MaStRMirror` builds upon methods provided in the class `MaStRDownload`. <br>

The aim of the class has been to mirror the Marktstammdatenregister database and keep it up-to-date.
Historically, `open-mastr` has been developed before the owner of the dataset, BNetzA, offered the `bulk` download.
The class can still be used for use-cases where only the most recent changes to a local database are of interest. 
For downloading the entire MaStR database we recommend the bulk download functionalities by specifying `donwload(method="bulk")`.



