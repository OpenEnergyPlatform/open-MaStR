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


Configuration files
-------------------

* :code:`credentials.cfg`: Credentials used to access
  `Marktstammdatenregister (MaStR) <https://www.marktstammdatenregister.de/MaStR>`_ API (read more in
  :ref:`MaStR account`) and token for Zenodo.
* :code:`data.yml`: Specify the name of the data version.
* :code:`filenames.yml`: File names are defined here.
* :code:`logging.yml`: Logging configuration. For changing the log level to increase or decrease details of log
  messages, edit the `level` of the handlers.
* :code:`tables.yml`: Names of tables where data gets imported to during :ref:`Post-processing`

Data
----

Resulting data of download, post-processing and analysis is saved under `$HOME/.open-MaStR/data/<data-version>`.
Files that are suffixed with `_raw` contain joined data retrieved during :ref:`downloading <Downloading raw data>`.
The structure of the data is described in :ref:`Data documentation`.

Logs
----

Logs are stored in a single file in `logs/`. New logging messages are appended. It is recommended to delete the log file
from time to time because of required disk space.


MaStR account
=============

For downloading data from the
`Marktstammdatenregister (MaStR) database <https://www.marktstammdatenregister.de/MaStR>`_
via its API a registration is mandatory (please read `here <https://www.marktstammdatenregister.de/MaStRHilfe/files/
regHilfen/201108_Handbuch%20f%C3%BCr%20Registrierungen%20durch%20Dienstleister.pdf>`_).

To download data using `open-MaStR` the credentials (MaStR user and token) need to be provided in a certain way.
Three options exist

* **Credentials file:** Both, user and token, are stored in plain text in the credentials file
  (`$HOME/.open-MaStR/config/credentials.cfg`)
* **Credentials file + keyring:** The user is stored in the credentials file, while the token is stored encrypted in
  the `keyring <https://pypi.org/project/keyring/>`_
* **Don't store:** Just use the password for one query and forget it

The latter option is only available when using :class:`open_mastr.soap_api.download.MaStRAPI`.
Instantiate with

.. code-block::

   MaStRAPI(user='USERNAME', key='TOKEN')

to provide user and token in a script and use these
credentials in subsequent queries.

For storing the credentials in the credentials file (plus optionally using keyring for the token) simply instantiate
:py:class:`open_mastr.soap_api.download.MaStRDownload` once and you get asked for a user name and a token.

It is also possible to create the credentials file by hand using this format

.. code-block::

    [MaStR]
    user = SOM123456789012
    token = msöiöo8u2o29933n31733m§=§1n33§304n... # optional, 540 characters

Read in the documentation of the `keyring library <https://pypi.org/project/keyring/>`_ how to store your token in the
keyring.


Zenodo token
============

Uploading data to `Zenodo <https://www.zenodo.org/>`_ requires authentication. When logged in with your account you can
`create tokens <https://zenodo.org/account/settings/applications/tokens/new/>`_ for API requests.

The section in `credentials.cfg` looks like:

.. code-block::

    [Zenodo]
    token = voh6Zo2ohbohReith4ec2iezeiJ9Miefohso0DohK9ohtha6mahfame7hohc