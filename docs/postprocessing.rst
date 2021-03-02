***************
Post-processing
***************

Pre-requisites
==============

Major parts of data cleansing, correction and enrichment are written in SQL. We recommend to run these scripts in a
dockered PostgreSQL database. If you want to use a native installation of PostgreSQL, help yourself and continue with
:ref:`Run postprocessing`.

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


Run postprocessing
==================

During post-processing downloaded :ref:`raw data <Downloading raw data>` gets cleanedimported to a PostgreSQL database,
 and enriched.
To run the postprocessing, use the following code snippets.

.. code-block:: python

   from open_mastr.postprocessing.cleaning import cleaned_data
   from postprocessing.postprocessing import postprocess

   cleaned = cleaned_data()
   postprocess(cleaned)

As a result, cleaned data gets saved as CSV files and  tables named like `bnetza_mastr_<technology>_cleaned`
appear in the schema `model_draft".
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


Database import
---------------

Where available, geo location data, given in lat/lon (*Breitengrad*, *Längengrad*), is converted into a PostGIS geometry
data type during database import. This allows spatial data operations in PostgreSQL/PostGIS.


Data cleansing
--------------

Units inside Germany and inside German offshore regions are selected and get distinguished from units that are (falsely)
located outside of Germany.
Data is stored in separate tables.


Data enrichment
---------------

For units without geo location data, a location is estimated based on the zip code. The centroid of the zip code region
polygon is used as proxy for the exact location.
To determine the zip code area, zip code data of OSM is used which is stored in
`boundaries.osm_postcode <https://openenergy-platform.org/dataedit/view/boundaries/osm_postcode>`_.
If a unit originally had correct geo data and the origin of estimated geom data is documented in the column `comment`.
