## Choose your own database


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

## Project directory

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

### Configuration files

* `credentials.cfg`: Credentials used to access
  `Marktstammdatenregister (MaStR) <https://www.marktstammdatenregister.de/MaStR>`_ API (read more in
  `MaStR account and credentials <MaStR account and credentials>`) and token for Zenodo.
* `filenames.yml`: File names are defined here.
* `logging.yml`: Logging configuration. For changing the log level to increase or decrease details of log
  messages, edit the `level` of the handlers.



### Logs

For the download via the API, logs are stored in a single file in `/$HOME/<user>/.open-MaStR/logs/open_mastr.log`.
New logging messages are appended. It is recommended to delete the log file from time to time because of its required disk space.


## Data

If the zipped dump of the MaStR is downloaded, it is saved in the folder `$HOME/.open-MaStR/data/xml_download`. New versions
of the dump overwrite older versions. 

The data can then be written to any sql database supported by [sqlalchemy](https://docs.sqlalchemy.org/). The type of the sql database is determined by the parameter `engine` in the [Mastr][open_mastr.Mastr] class.

For more information regarding the database see :ref:`Database settings <Database settings>`.

Exported data is saved under `$HOME/.open-MaStR/data/<data-version>`.
Files that are suffixed with `_raw` can contain joined data retrieved during :ref:`downloading <Download>`.
The structure of the data is described in :ref:`Data Description`.


