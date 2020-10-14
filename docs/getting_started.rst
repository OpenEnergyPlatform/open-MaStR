***************
Getting started
***************

For using open-MaStR download, process or analyze utilities, you need to :ref:`install <Installation>` the package and
configure your :ref:`setup <Setup and configuration>`. For downloading the Marktstammdatenregister (MaStR) raw data, additionally, a
:ref:`MaStR-account <Creating a MaStR account>` is required.

Installation
============

Setup and configuration
=======================

The directory `$HOME/.open-MaStR` is automatically created. It is used to store configuration files and save data.
Default config files are copied to this directory which can be modified - but with caution.

Credentials
-----------

Credentials for the MaStR-API are stored in `$HOME/.open-MaStR/config/credentials.cfg` (plain text).
If this file does not exist, you're asked to enter your data on the command-line.

Data
----

Resulting data of download, post-processing and analysis is saved under `$HOME/.open-MaStR/data/<data-version>`.
Files suffixed with `_raw` contain merged data.
File names of these files are defined by `$HOME/.open-MaStR/config/filenames.yml`.
The name of the data version and total number of available units can be set in `$HOME/.open-MaStR/config/data.yml`.

Creating a MaStR account
========================
