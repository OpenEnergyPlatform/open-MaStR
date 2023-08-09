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
    │   └── tables.yml
    ├── data
    │   ├── rli_v3.0.0
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
* `tables.yml`: Names of tables where data gets imported to during :ref:`Post-processing (Outdated)`


### Logs

For the download via the API, logs are stored in a single file in `/$HOME/cwm/.open-MaStR/logs/open_mastr.log`.
New logging messages are appended. It is recommended to delete the log file from time to time because of its required disk space.


## Data

If the zipped dump of the MaStR is downloaded, it is saved in the folder `$HOME/.open-MaStR/data/xml_download`. New versions
of the dump overwrite older versions. 

The data can then be written to any sql database supported by [sqlalchemy](https://docs.sqlalchemy.org/). The type of the sql database is determined by the parameter `engine` in the [Mastr][open_mastr.Mastr] class.

For more information regarding the database see :ref:`Database settings <Database settings>`.

Exported data is saved under `$HOME/.open-MaStR/data/<data-version>`.
Files that are suffixed with `_raw` can contain joined data retrieved during :ref:`downloading <Download>`.
The structure of the data is described in :ref:`Data Description`.


## Environment variables


There are some environment variables to customize open-MaStR:


| variable | description | example |
| :--------: | :--------: | :--------: |
| SQLITE_DATABASE_PATH |Path to the SQLite file. This allows to use to use multiple instances of the MaStR database. The database instances exist in parallel and are independent of each other. | `/home/mastr-rabbit/.open-MaStR/data/sqlite/your_custom_instance_name.db` |



## Zenodo token
!!! danger
    Do we want users to do this?
Uploading data to `Zenodo <https://www.zenodo.org/>`_ requires authentication. When logged in with your account you can
`create tokens <https://zenodo.org/account/settings/applications/tokens/new/>`_ for API requests.

The section in `credentials.cfg` looks like:

.. code-block::

    [Zenodo]
    token = voh6Zo2ohbohReith4ec2iezeiJ9Miefohso0DohK9ohtha6mahfame7hohc