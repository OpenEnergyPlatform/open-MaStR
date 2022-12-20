********************
Advanced Topics
********************
In the following sections, a deeper description of the package and its functionalities is given.

Configuration
==================
Project directory
-------------------

The directory `$HOME/.open-MaStR` is automatically created. It is used to store configuration files and save data.
Default config files are copied to this directory which can be modified - but with caution.
The project home directory is structured as follows (files and folders below `data/` just an example).

.. code-block:: bash

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


Configuration files
-------------------

* :code:`credentials.cfg`: Credentials used to access
  `Marktstammdatenregister (MaStR) <https://www.marktstammdatenregister.de/MaStR>`_ API (read more in
  :ref:`MaStR account and credentials <MaStR account and credentials>`) and token for Zenodo.
* :code:`filenames.yml`: File names are defined here.
* :code:`logging.yml`: Logging configuration. For changing the log level to increase or decrease details of log
  messages, edit the `level` of the handlers.
* :code:`tables.yml`: Names of tables where data gets imported to during :ref:`Post-processing (Outdated)`

Data
----

If the zipped dump of the MaStR is downloaded, it is saved in the folder `$HOME/.open-MaStR/data/xml_download`. New versions
of the dump overwrite older versions. 

The data can then be written to an sql database. The type of the sql database is determined
by the parameter `engine` in the Mastr class (see :ref:`mastr module`). The possible databases are:

* sqlite: By default the database will be stored in `$HOME/.open-MaStR/data/sqlite/open-mastr.db` (can be customized via the
  :ref:`environment variable <Environment variables>` `$SQLITE_DATABASE_PATH`).
* docker-postgres: A docker container of a PostgreSQL database. 
* own database: The Mastr class accepts a sqlalchemy.engine.Engine object as engine which enables the user to 
  use any other desired database.
.. tabs::

    .. code-tab:: py SQLite

        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///path/to/sqlite/database.db")
        db = Mastr(engine=engine)

    .. code-tab:: py PostgreSQL

        from sqlalchemy import create_engine

        engine = create_engine("postgresql://myusername:mypassword@localhost/mydatabase"
        db = Mastr(engine=engine)
..

Resulting data of download, post-processing and analysis is saved under `$HOME/.open-MaStR/data/<data-version>`.
Files that are suffixed with `_raw` contain joined data retrieved during :ref:`downloading <Download>`.
The structure of the data is described in :ref:`Data Description`.

Environment variables
^^^^^^^^^^^^^^^^^^^^^

There are some environment variables to customize open-MaStR:

.. list-table::
   :widths: 5 5 5
   :header-rows: 1

   * - variable
     - description
     - example
   * - SQLITE_DATABASE_PATH
     - Path to the SQLite file. This allows to use to use multiple instances of the MaStR database. The database instances exist in parallel and are independent of each other.
     - `/home/mastr-rabbit/.open-MaStR/data/sqlite/your_custom_instance_name.db`

MaStR account and credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For downloading data from the
`Marktstammdatenregister (MaStR) database <https://www.marktstammdatenregister.de/MaStR>`_
via its API a registration is mandatory (please `read here <https://www.marktstammdatenregister.de/MaStRHilfe/files/
regHilfen/201108_Handbuch%20f%C3%BCr%20Registrierungen%20durch%20Dienstleister.pdf>`_).

To download data using `open-MaStR`, the credentials (MaStR user and token) need to be provided in a certain way.
Three options exist

1. **Credentials file:** Both, user and token, are stored in plain text in the credentials file


For storing the credentials in the credentials file (plus optionally using keyring for the token) simply instantiate
:py:class:`open_mastr.soap_api.download.MaStRDownload` once and you get asked for a user name and a token. The
information you insert will be used to create the credentials file.

It is also possible to create the credentials file by hand, using this format:

.. code-block::

    [MaStR]
    user = SOM123456789012
    token = msöiöo8u2o29933n31733m§=§1n33§304n... # optional, 540 characters

The `token` should be written in one line, without line breaks.

The credentials file needs to be stored at: `$HOME/.open-MaStR/config/credentials.cfg`

2. **Credentials file + keyring:** The user is stored in the credentials file, while the token is stored encrypted in the `keyring <https://pypi.org/project/keyring/>`_.

Read in the documentation of the `keyring library <https://pypi.org/project/keyring/>`_ how to store your token in the
keyring.

3. **Don't store:** Just use the password for one query and forget it

The latter option is only available when using :class:`open_mastr.soap_api.download.MaStRAPI`.
Instantiate with

.. code-block::

   MaStRAPI(user='USERNAME', key='TOKEN')

to provide user and token in a script and use these
credentials in subsequent queries.

Logs
----

For the download via the API, logs are stored in a single file in `/$HOME/cwm/.open-MaStR/logs/open_mastr.log`.
New logging messages are appended. It is recommended to delete the log file from time to time because of its required disk space.


Zenodo token
------------------

Uploading data to `Zenodo <https://www.zenodo.org/>`_ requires authentication. When logged in with your account you can
`create tokens <https://zenodo.org/account/settings/applications/tokens/new/>`_ for API requests.

The section in `credentials.cfg` looks like:

.. code-block::

    [Zenodo]
    token = voh6Zo2ohbohReith4ec2iezeiJ9Miefohso0DohK9ohtha6mahfame7hohc


Download
=============================

Get data via the bulk download
-------------------------------
On the homepage `MaStR/Datendownload <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_ a zipped folder containing the whole 
MaStR is offered. The data is delivered as xml-files. The official documentation can be found 
`here [in german] <https://www.marktstammdatenregister.de/MaStRHilfe/files/gesamtdatenexport/Dokumentation%20MaStR%20Gesamtdatenexport.pdf>`_. 
This data is updated on a daily base. 

.. figure:: images/MaStR_bulk_download.svg
   :width: 50%
   :align: center
   
   Overview of the open_mastr bulk download functionality.

In the following, the process is described that is started when calling the download function with the parameter method="bulk". 
First, the zipped files are downloaded and saved in `$HOME/.open-MaStR/data/xml_download`. The zipped folder contains many xml files,
which represent the different tables from the MaStR. Those tables are then parsed to a sqlite database. If only some specific
data are of interest, they can be specified with the parameter `data`. Every table that is selected in `data` will be deleted, if existent,
and then filled with data from the xml files.

In the last step, a basic data cleansing is performed. Many entries in the MaStR from the bulk download are replaced by numbers.
As an example, instead of writing the german states where the unit is registered (Saxony, Brandenburg, Bavaria, ...) the MaStR states 
corresponding digits. One major step of cleansing is therefore to replace those digits with their original meaning. 
Moreover, the datatypes of different entries are set in the data cleansing process.

Advantages of the bulk download:
 * No registration for an API key is needed

Disadvantages of the bulk download:
 * No single tables or entries can be downloaded

Get data via the MaStR-API
---------------------------

Downloading data with the MaStR-API is more complex than using the :ref:`Bulk download <Bulk download>`.

Follow this checklist for configuration:

#. Set up a MaStR account and configure your :ref:`credentials <MaStR account and credentials>`.
#. Configure your :ref:`database <Database settings>`.
#. Configure your :ref:`API download <API download>` settings.

Database settings
^^^^^^^^^^^^^^^^^^^^^^^^

Configure your database with the `engine` parameter of :class:`Mastr`.
It defines the engine of the database where the MaStR is mirrored to. Default is 'sqlite'.

Choose from: {'sqlite', 'docker-postgres', sqlalchemy.engine.Engine}


API download settings
^^^^^^^^^^^^^^^^^^^^^^^^

Prior to starting the download of data from MaStR-API, you might want to adjust parameters in the config file.
Please read in :ref:`Configuration`.
For downloading data from Marktstammdatenregister (MaStR) registering an account is required.
Find more information :ref:`here <MaStR account and credentials>`.

By using the API (e.g. by setting `method` ="API" in the `Mastr.download()` method)
additional parameters can be set to define in detail which data should be obtained.

.. list-table:: API-related download arguments and explanation
   :widths: 5 5 5
   :header-rows: 1

   * - argument
     - options for specification
     - explanation
   * - data
     - ["wind","biomass","combustion","gsgk","hydro","nuclear","storage","solar"]
     - Select data to download.
   * - date
     - None or :class:`datetime.datetime` or str
     - Specify backfill date from which on data is retrieved. Only data with time stamp greater than `date` will be retrieved. Defaults to `None` which means that todays date is chosen.
   * - api_data_types
     - ["unit_data","eeg_data","kwk_data","permit_data"]
     - Select the type of data to download.
   * - api_location_types
     - ["location_elec_generation","location_elec_consumption","location_gas_generation","location_gas_consumption"]
     - Select location_types to download.
   * - api_processes
     - Number of type int, e.g.: 5
     - Select the number of parallel download processes. Possible number depends on the capabilities of your machine. Defaults to `None`.
   * - api_limit
     - Number of type int, e.g.: 1500
     - Select the number of entries to download. Defaults to 50.
   * - api_chunksize
     - int or None, e.g.: 1000
     - Data is downloaded and inserted into the database in chunks of `api_chunksize`. Defaults to 1000.

.. warning::
    The implementation of parallel processes is currently under construction. Please let the argument `api_processes` at the default value `None`.





Three different levels of access to data are offered where the code builds on top of each other.

.. figure:: images/MaStR_downloading.svg
   :width: 70%
   :align: center
   
   Overview of open-mastr API download functionality.

The most fundamental data access is provided by :class:`open_mastr.soap_api.download.MaStRAPI` that simply
wraps SOAP webservice functions with python methods.
Using the methods of the aforementioned, :class:`open_mastr.soap_api.download.MaStRDownload` provides 
methods for bulk data download and association of data from different sources.
If one seeks for an option to store the entire data in a local database, 
:class:`open_mastr.soap_api.mirror.MaStRMirror` is the right choice. It offers complete data download 
and updating latest data changes.

Mirror MaStR database
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: open_mastr.soap_api.mirror.MaStRMirror
   :members:


Download large number of units
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Downloading thousands of units is simplified using
:meth:`open_mastr.soap_api.download.MaStRDownload.download_power_plants`.

.. autoclass:: open_mastr.soap_api.download.MaStRDownload
   :members:


MaStR API wrapper
^^^^^^^^^^^^^^^^^^^^

For simplifying queries against the MaStR API, :class:`open_mastr.soap_api.download.MaStRAPI` wraps around exposed
SOAP queries.
This is the low-level MaStR API access.

.. autoclass:: open_mastr.soap_api.download.MaStRAPI
   :members:


.. include:: _data/raw_data.rst


Post-processing (Outdated)
===========================

The following chapter on postprocessing is outdated. It refers to jupyter notebooks you can find 
on our `github page <https://github.com/OpenEnergyPlatform/open-MaStR>`_. It is planned to add those post-processing
features in later versions to the open_mastr package.

Pre-requisites (Outdated)
-------------------------

Major parts of data cleansing, correction and enrichment are written in SQL. We recommend to run these scripts in a
dockered PostgreSQL database. If you want to use a native installation of PostgreSQL, help yourself and continue with
:ref:`Run postprocessing (Outdated)`.

Make sure you have `docker-compose <https://docs.docker.com/compose/install/>`_ installed. Run

.. code-block:: bash

   docker-compose up -d

to start a PostgreSQL database.

You can connect to the database with the following credentials (if not changed in `docker-compose.yml`).

======== ==========
Field    Value
======== ==========
host     localhost
port     55443
database open-mastr
User     open-mastr
Password open-mastr
======== ==========


Once you're finished with working in/with the database, shut it down with

.. code-block:: bash

   docker-compose down


Run postprocessing (Outdated)
-----------------------------

During post-processing downloaded :ref:`Download <Download>` gets cleaned, imported to a PostgreSQL database,
and enriched.
To run the postprocessing, use the following code snippets.

.. code-block:: python

   from open_mastr.postprocessing.cleaning import cleaned_data
   from postprocessing.postprocessing import postprocess

   cleaned = cleaned_data()
   postprocess(cleaned)

As a result, cleaned data gets saved as CSV files and  tables named like `bnetza_mastr_<technology>_cleaned`
appear in the schema `model_draft`.
Use

.. code-block:: python

   from postprocessing.postprocessing import to_csv

   to_csv()

to export processed data in CSV format.

.. note::

   It is assumed raw data resides in `~/.open-MaStR/data/<data version>/` as explained in :ref:`Configuration`.

.. warning::

   Raw data downloaded with :class:`open_mastr.soap_api.download.MaStRDownload` is
   currently not supported.
   Please use raw data from a CSV export(:meth:`open_mastr.soap_api.mirror.MaStRMirror.to_csv`)
   of :class:`open_mastr.soap_api.mirror.MaStRMirror` data.


Database import (Outdated)
--------------------------

Where available, geo location data, given in lat/lon (*Breitengrad*, *Längengrad*), is converted into a PostGIS geometry
data type during database import. This allows spatial data operations in PostgreSQL/PostGIS.


Data cleansing (Outdated)
-------------------------

Units inside Germany and inside German offshore regions are selected and get distinguished from units that are (falsely)
located outside of Germany.
Data is stored in separate tables.


Data enrichment (Outdated)
--------------------------

For units without geo location data, a location is estimated based on the zip code. The centroid of the zip code region
polygon is used as proxy for the exact location.
To determine the zip code area, zip code data of OSM is used which is stored in
`boundaries.osm_postcode <https://openenergy-platform.org/dataedit/view/boundaries/osm_postcode>`_.
If a unit originally had correct geo data and the origin of estimated geom data is documented in the column `comment`.


Zenodo upload
=============

Use the function :func:`~.open_mastr.data_io.zenodo_upload` for uploading data to Zenodo.

.. autofunction:: open_mastr.data_io.zenodo_upload