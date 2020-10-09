***************
Post-processing
***************

Pre-requisites
==============

Major parts of data cleansing, correction and enrichment are written in SQL. We recommend to run these scripts in a
dockered PostgreSQL database. If you want to use a native installation of PostgreSQL, continue with
:ref:`Run postprocessing`.

Make sure you have `docker-compose <https://docs.docker.com/compose/install/>`_ installed. Run

.. code-block:: bash

   docker-compose up -d

to start a PostgreSQL database.

Once you're finished with work in the database, shut it down with

.. code-block:: bash

   docker-compose down


Run postprocessing
==================