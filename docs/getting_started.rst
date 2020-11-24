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

There are three options to provide/store credentials for the MaStR-API

.. csv-table:: Configuration parameters MaStR downloader
   :header: "Choice #", "Name", "Description"
   :widths: 3, 5, 30

   "\(1\)", "Keyring", "Use the `keyring library <https://pypi.org/project/keyring/>`_ to store the password encrypted."
   "\(2\)", "Config file", "Use the config file (`$HOME/.open-MaStR/config/credentials.cfg`) to store the password in plain text."
   "\(0\)", "Don't store", "Just use the password for one query and forget it."

During instantiation of :py:class:`open_mastr.soap_api.download.MaStRDownload` you get asked for a user name and
a token, see :ref:`Download`.

There is also the option to provide user and password to :py:class:`open_mastr.soap_api.download.MaStRAPI`.
Instantiate with :code:`MaStRAPI(user='USERNAME', key='TOKEN')` to provide user and password in a script.

Data
----

Resulting data of download, post-processing and analysis is saved under `$HOME/.open-MaStR/data/<data-version>`.
Files suffixed with `_raw` contain merged data.
File names of these files are defined by `$HOME/.open-MaStR/config/filenames.yml`.
The name of the data version and total number of available units can be set in `$HOME/.open-MaStR/config/data.yml`.

Creating a MaStR account
========================
