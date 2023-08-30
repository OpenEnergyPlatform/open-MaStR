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
    │       ├── bnetza_mastr_solar_eeg_fail.csv
    │       ├── bnetza_mastr_solar_extended_fail.csv
    │       └── bnetza_mastr_solar_raw.csv
    │   ├── sqlite
    │       └── open-mastr.db
        └── xml_download
            └── Gesamtdatenexport.zip
    └── logs
        └── open_mastr.log
```

* `credentials.cfg`: Credentials used to access
  [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) API
* `filenames.yml`: File names are defined here.
* `logging.yml`: Logging configuration. For changing the log level to increase or decrease details of log
  messages, edit the `level` of the handlers.
* `dataversion` <br>
  Exported data is as csv files from [`to_csv`][open_mastr.Mastr.to_csv]
* `Gesamtdatenexport_<date>.zip` <br>
  New versions of the dump overwrite older versions. 



### Logs

For the download via the API, logs are stored in a single file in `/$HOME/<user>/.open-MaStR/logs/open_mastr.log`.
New logging messages are appended. It is recommended to delete the log file from time to time because of its required disk space.


### Data

If the zipped dump of the MaStR is downloaded, it is saved in the folder `$HOME/.open-MaStR/data/xml_download`. 

The data can then be written to any sql database supported by [sqlalchemy](https://docs.sqlalchemy.org/). The type of the sql database is determined by the parameter `engine` in the [Mastr][open_mastr.Mastr] class.

For more information regarding the database see [Database settings](Database settings).


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
via its API a [registration](https://www.marktstammdatenregister.de/MaStRHilfe/files/regHilfen/201108_Handbuch%20f%C3%BCr%20Registrierungen%20durch%20Dienstleister.pdf) is mandatory.

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

    The latter option is only available when using :class:`open_mastr.soap_api.download.MaStRAPI`.
    Instantiate with

    ```python
    MaStRAPI(user='USERNAME', key='TOKEN')
    ```

    to provide user and token in a script and use these
    credentials in subsequent queries.

### Mastr API

chrwm

### MastrDownload

chrwm

### MastrMirror

chrwm 
Decide to include or not - depending on use


