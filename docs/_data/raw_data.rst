Data Description
=====================

In the following, we will describe the data retrieved from the MaStR database. The two figures showing the data model are 
taken from `here <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/dokumentendownload.html>`_.


.. figure:: /images/ObjektmodellMaStR.png
   :width: 70%
   :align: center
   
   Overview of MaStR data model and its sub-categories.

As can be seen from the first figure, the MaStR can be divided into three sub-categories: Actor (Marktakteur), grid (Netz),
and unit (Einheit). Since the technological units are the core of the MaStR, their data model is shown in the next figure 
with a higher level of detail. 

.. figure:: /images/DetailAnlagenModellMaStR.png
   :width: 90%
   :align: center
   
   Overview of MaStR data model with a focus on the electricity and gas units.




The MaStR comes in the form of a relational database, where the information within the database is structured into various :ref:`tables <Accessing the database>`.
Below we have summarized the attributes of the exported csv tables together with a short description (Only in German).

solar
-------

.. csv-table::
   :file: raw/bnetza_mastr_solar_raw.csv
   :widths: 20, 35, 15, 30
   :header-rows: 1


wind
-------

.. csv-table::
   :file: raw/bnetza_mastr_wind_raw.csv
   :widths: 20, 35, 15, 15
   :header-rows: 1


biomass
-------

.. csv-table::
   :file: raw/bnetza_mastr_biomass_raw.csv
   :widths: 20, 35, 15, 15
   :header-rows: 1


combustion
------------

.. csv-table::
   :file: raw/bnetza_mastr_combustion_raw.csv
   :widths: 20, 35, 15, 15
   :header-rows: 1


gsgk
-------

.. csv-table::
   :file: raw/bnetza_mastr_gsgk_raw.csv
   :widths: 20, 35, 15, 15
   :header-rows: 1


hydro
-------

.. csv-table::
   :file: raw/bnetza_mastr_hydro_raw.csv
   :widths: 20, 35, 15, 15
   :header-rows: 1


nuclear
-------

.. csv-table::
   :file: raw/bnetza_mastr_nuclear_raw.csv
   :widths: 20, 35, 15, 15
   :header-rows: 1


storage
-------

.. csv-table::
   :file: raw/bnetza_mastr_storage_raw.csv
   :widths: 20, 35, 15, 15
   :header-rows: 1


