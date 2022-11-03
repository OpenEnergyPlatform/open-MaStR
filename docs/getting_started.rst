********************
Getting Started
********************

The intention of open-MaStR is to provide tools for receiving a complete as possible and accurate as possible list of
power plant units based on the public registry Marktstammdatenregister (short: `MaStR <https://www.marktstammdatenregister.de>`_).

Downloading the MaStR data
=============================

For downloading the MaStR and saving
it in a sqlite database, you will use the :class:`Mastr` class and its `download` method (For documentation of those methods see
:ref:`mastr module`)

The :meth:`download` function offers two different ways to get the data by changing the `method` parameter (if not specified, `method` defaults to "bulk"):
 #. `method` = "bulk": Get data via the bulk download from `MaStR/Datendownload <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_
 #. `method` = "API": Get data via the MaStR-API

Keep in mind: While the data from both methods is quiet similar, it is not exactly the same!

Bulk download
-----------------------------------
The following code block shows the basic download commands:

    .. code-block:: python

       from open_mastr import Mastr

       db = Mastr()
       db.download()

When a :class:`Mastr` object is initialized, a sqlite database is created
in `$HOME/.open-MaStR/data/sqlite`. With the function `Mastr.download()`, the **whole MaStR is downloaded** in the zipped xml file 
format. It is then read into the sqlite database and simple data cleansing functions are started.

More detailed information can be found in :ref:`Get data via the bulk download <Get data via the bulk download>`.

API download
-----------------------------------
When using `download(method="API")`, the data is retrieved from the MaStR API. For using the MaStR API,
credentials are needed (see :ref:`Get data via the MaStR-API`). By using the API,
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
   * - api_date
     - None or :class:`datetime.datetime` or str
     - Specify backfill date from which on data is retrieved. Only data with time stamp greater that `api_date` will be retrieved. Defaults to `None`.
   * - api_chunksize
     - int or None, e.g.: 1000
     - Data is downloaded and inserted into the database in chunks of `api_chunksize`. Defaults to 1000.

.. warning::
    The implementation of parallel processes is currently under construction. Please let the argument `api_processes` at the default value `None`.

The default settings will download retrieved data into the sqlite database. The function can be used to mirror the open-MaStR database regularly
without needing to download the `provided dumps <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_  daily.

More detailed information can be found in :ref:`Get data via the MaStR-API <Get data via the MaStR-API>`.

Accessing the database
===================================


For accessing and working with the MaStR database after you have downloaded it, you can use sqlite browsers 
(such as `DB Browser for SQLite <https://sqlitebrowser.org/>`_) or any python module
which can process sqlite data. Pandas, for example, comes with the function
`read_sql <https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html>`_.

    .. code-block:: python

      import pandas as pd

      table="wind_extended"
      df = pd.read_sql(sql=table, con=db.engine)


The tables that exist in the database are listed below. Their relations can be found in :ref:`Data Description <Data Description>`

.. list-table:: Tables in the sqlite database
  :widths: 5
  :header-rows: 1

  * - table
  * - additional_data_requested
  * - additional_locations_requested
  * - balancing_area
  * - basic_units
  * - biomass_eeg
  * - biomass_extended
  * - combustion_extended
  * - electricity_consumer
  * - gas_consumer
  * - gas_producer
  * - gas_storage
  * - gas_storage_extended
  * - grid_connections
  * - grids
  * - gsgk_eeg
  * - gsgk_extended
  * - hydro_eeg
  * - hydro_extended
  * - kwk
  * - locations_basic
  * - locations_extended
  * - market_actors
  * - market_roles
  * - missed_additional_data
  * - missed_extended_location_data
  * - nuclear_extended
  * - permit
  * - solar_eeg
  * - solar_extended
  * - sqlite_sequence
  * - storage_eeg
  * - storage_extended
  * - storage_units
  * - wind_eeg
  * - wind_extended

Exporting data
===================================


The tables in the database can be exported as csv files. While technology-related data is joined for each unit,
additional tables are mirrored from database to csv as they are. To export the data you can use to method :meth:`to_csv`

    .. code-block:: python

      tables=["wind", "grid"]
      db.to_csv(tables)

