********************
Downloading raw data
********************

The intention of open-MaStR is to provide tools for receiving a complete as possible and accurate as possible list of
power plant units based on the public registry Marktstammdatenregister (short: `MaStR <https://www.marktstammdatenregister.de>`_). 
For downloading the MaStR and saving 
it in a sqlite database, you will use the `MaStR` class and its `download` method.

.. autoclass:: open_mastr.mastr.Mastr
   :members:

As you see, the `download` function offers two different ways by changing the method parameter:
1. method = "bulk": Get data via the bulk download from `MaStR/Datendownload <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_
2. method = "API": Get data via the MaStR-API

Keep in mind: While the data from both methods is quiet similar, it is not exactly the same!

Get data via the bulk download
-------------------------------
On the homepage `MaStR/Datendownload <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_ a zipped folder containing the whole 
MaStR is offered. The data is delivered as xml-files. The official documentation can be found 
`here [in german] <https://www.marktstammdatenregister.de/MaStRHilfe/files/gesamtdatenexport/Dokumentation%20MaStR%20Gesamtdatenexport.pdf>`_. 
This data is updated on a daily base.

Advantages of the bulk download:
* No registration for an API key is needed

Disantvantages of the bulk download:
* No single tables or entries can be downloaded


Get data via the MaStR-API
---------------------------
Prior to starting the download of data from MaStR-API, you might want to adjust parameters in the config file.
Please read in :ref:`Configuration`.
For downloading data from Marktstammdatenregister (MaStR) registering an account is required.
Find more information :ref:`here <MaStR account>`.

Three different levels of access to data are offered where the code builds on top of each other.

.. figure:: images/MaStR_downloading.svg
   :width: 70%
   :align: center
   
   Overview of open-MaStR download functionality.

The most fundamental data access is provided by :class:`open_mastr.soap_api.download.MaStRAPI` that simply
wraps SOAP webservice functions with python methods.
Using the methods of the aforementioned, :class:`open_mastr.soap_api.download.MaStRDownload` provides 
methods for bulk data download and association of data from different sources.
If one seeks for an option to store the entire data in a local database, 
:class:`open_mastr.soap_api.mirror.MaStRMirror` is the right choice. It offers complete data download 
and updating latest data changes.

Mirror MaStR database
=====================

.. autoclass:: open_mastr.soap_api.mirror.MaStRMirror
   :members:


Bulk download
=============

Downloading thousands of units is simplified using
:meth:`open_mastr.soap_api.download.MaStRDownload.download_power_plants`.

.. autoclass:: open_mastr.soap_api.download.MaStRDownload
   :members:


MaStR API wrapper
=================

For simplifying queries against the MaStR API, :class:`open_mastr.soap_api.download.MaStRAPI` wraps around exposed
SOAP queries.
This is the low-level MaStR API access.

.. autoclass:: open_mastr.soap_api.download.MaStRAPI
   :members:
