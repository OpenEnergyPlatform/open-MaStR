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


Another option is the download of data via the MaStR soap API. 

    .. code-block:: python

           db.download(method="API")

This requires an account and token (see :ref:`configuration <Configuration>`).
The download via the API has the advantage, that **single entries of the MaStR can be downloaded** (one does not need to download the whole MaStR).

Some Basic settings for configuring the `API` download are:

    .. code-block:: python

        technology = [
            "wind",
            "biomass",
            "combustion",
            "gsgk",
            "hydro",
            "nuclear",
            "storage",
            "solar",
        ]

        api_data_types = [
            "unit_data",
            "eeg_data",
            "kwk_data",
            "permit_data"
        ]

        api_location_types = [
            "location_elec_generation",
            "location_elec_consumption",
            "location_gas_generation",
            "location_gas_consumption",
        ]

        db.download(method="API", technology=technology, api_data_types=api_data_types, api_location_types=api_location_types)

Here, the technologies, data_types and location_types that are not of interest can be deleted from the list.
Detailed information can be found in :ref:`Downloading the MaStR data <Downloading the MaStR data>`.


Accessing the database
------------------------

For accessing and working with the MaStR database after you have downloaded it, you can use any python module 
which can process sqlite data. Pandas, for example, comes with the function 
`read_sql <https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html>`_.