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

The main entry point to open_mastr is the :class:`MaStR` class. When it is initialized, a sqlite database is created 
in `$HOME/.open-MaStR/data/sqlite`. With the function `Mastr.download()`, the **whole MaStR is downloaded** in the zipped xml file 
format. It is then read into the sqlite database and simple data cleansing functions are started.


Another option is the download of data via the MaStR soap API. 

    .. code-block:: python

           db.download(method="API")

This requires an account and token (see :ref:`configuration <Configuration>`).
The download via the API has the advantage, that **single entries of the MaStR can be downloaded** (one does not need to download the whole MaStR).
Detailed information can be found in :ref:`downloading <Downloading raw data>`.