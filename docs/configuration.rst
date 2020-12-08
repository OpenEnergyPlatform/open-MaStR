*************
Configuration
*************


Project directory
=================

The directory `$HOME/.open-MaStR` is automatically created. It is used to store configuration files and save data.
Default config files are copied to this directory which can be modified - but with caution.
The project home directory is structured as follows (files and folders below `data/` just an example).

.. code-block:: bash

    .open-MaStR/
    ├── config
    │   ├── credentials.cfg
    │   ├── data.yml
    │   ├── filenames.yml
    │   └── tables.yml
    ├── data
    │   └── rli_v3.0.0
    │       ├── bnetza_mastr_solar_eeg_fail.csv
    │       ├── bnetza_mastr_solar_extended_fail.csv
    │       └── bnetza_mastr_solar_raw.csv
    └── logs
        └── open_mastr.log


Configuration files
-------------------

* :code:`credentials.cfg`: Credentials used to access
  `Marktstammdatenregister (MaStR) <https://www.marktstammdatenregister.de/MaStR>`_ API.
  Read more in :ref:`MaStR account`.
* :code:`data.yml`: Specify the name of the data version.
* :code:`filenames.yml`: File names are defined here.
* :code:`tables.yml`: Names of tables where data gets imported to during :ref:`Post-processing`

Data
----

Resulting data of download, post-processing and analysis is saved under `$HOME/.open-MaStR/data/<data-version>`.
Files that are suffixed with `_raw` contain joined data retrieved during :ref:`downloading <Downloading raw data>`.

Logs
----

Logs are stored in a single file in `logs/`. New logging messages are appended. It is recommended to delete the log file
from time to time because of required disk space.

