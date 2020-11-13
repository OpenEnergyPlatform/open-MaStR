********************
Downloading raw data
********************

The intention of open-MaStR is to provide tools to receiving a complete as possible and accurate as possible list of
power plant units based on `MaStR <https://www.marktstammdatenregister.de>`_.
Therefore, in particular, methods for bulk download of the entire MaStR database are provided.

Configuration
=============

Prior to starting the download of data from MaStR-API, you might want to adjust parameters in the config file.
A default configuration is copied to `~/open-MaStR/config/`.


Download
========

Use the method :meth:`open_mastr.soap_api.download.MaStRDownload.download_power_plants` from
:class:`open_mastr.soap_api.download.MaStRDownload` to download power plant data.

.. code-block:: python

    from open_mastr.soap_api.download import MaStRDownload

    mastr_dl = MaStRDownload()

    for tech in ["nuclear", "hydro", "wind", "solar", "biomass", "combustion", "gsgk"]:
        power_plants = mastr_dl.download_power_plants(tech, limit=10)
        print(power_plants.head())

.. warning::

    Be careful with increasing `limit`. Typically, your account allows only for 10.000 API request per day.


Resulting data is saved to `~/open-MaStR/config/<version>`.