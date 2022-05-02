********************
Getting Started
********************

The intention of open-MaStR is to provide tools for receiving a complete as possible and accurate as possible list of
power plant units based on the public registry Marktstammdatenregister (short: `MaStR <https://www.marktstammdatenregister.de>`_).
The following code block shows the basic commands:

    .. code-block:: python

       from open_mastr import Mastr

       db = Mastr()
       db.download()

The main entry point to open_mastr is the :class:`Mastr` class (see :ref:`mastr module`). When it is initialized, a sqlite database is created
in `$HOME/.open-MaStR/data/sqlite`. With the function `Mastr.download()`, the **whole MaStR is downloaded** in the zipped xml file 
format. It is then read into the sqlite database and simple data cleansing functions are started.


The arguments in the `download()` function for configuring the `API` download are:

.. list-table:: API-related download arguments and explanantion
   :widths: 5 5 5
   :header-rows: 1

   * - argument
     - options for specification
     - explanation
   * - technology
     - ["wind","biomass","combustion","gsgk","hydro","nuclear","storage","solar"]
     - Select technologies to download.
   * - api_data_types
     - ["unit_data","eeg_data","kwk_data","permit_data"]
     - Select the type of data to download.
   * - api_location_types
     - ["location_elec_generation","location_elec_consumption","location_gas_generation","location_gas_consumption"]
     - Select location_types to download.
   * - api_processes
     - Number of type int, e.g.: 5
     - Select the number of parallel download processes. Possible number depends on the capabilities of your machine. Defaults to `Ǹone`.
   * - api_limit
     - Number of type int, e.g.: 1500
     - Select the number of entries to download. Defaults to 50.
   * - api_date
     - None or :class:`datetime.datetime` or str
     - Specify backfill date from which on data is retrieved. Only data with time stamp greater that `api_date` will be retrieved. Defaults to `Ǹone`.
   * - api_chunksize
     - int or None, e.g.: 1000
     - Data is downloaded and inserted into the database in chunks of `api_chunksize`. Defaults to 1000.


More detailed information can be found in :ref:`Downloading the MaStR data <Downloading the MaStR data>`.


Accessing the database
------------------------

For accessing and working with the MaStR database after you have downloaded it, you can use any python module 
which can process sqlite data. Pandas, for example, comes with the function 
`read_sql <https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html>`_.