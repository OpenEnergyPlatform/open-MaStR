********************
Downloading raw data
********************

The intention of open-MaStR is to provide tools to receiving a complete as possible and accurate as possible list of
power plant units based on `MaStR <https://www.marktstammdatenregister.de>`_.
Therefore, in particular, methods for bulk download of the entire MaStR database are provided.

Prior to starting the download of data from MaStR-API, you might want to adjust parameters in the config file.
Please read in :ref:`Configuration`.
For downloading data from Marktstammdatenregister (MaStR) registering an account is required.
Find more information :ref:`here <MaStR account>`.

Bulk download
=============

Downloading thousands of units is simplified using
:meth:`open_mastr.soap_api.download.MaStRDownload.download_power_plants`.

.. autoclass:: open_mastr.soap_api.download.MaStRDownload
   :members:

   .. automethod:: __init__


MaStR API wrapper
=================

For simplifying queries against the MaStR API, :class:`open_mastr.soap_api.download.MaStRAPI` wraps around exposed
SOAP queries.
This is the low-level MaStR API access.

.. autoclass:: open_mastr.soap_api.download.MaStRAPI
   :members: