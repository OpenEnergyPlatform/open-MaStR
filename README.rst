
.. image:: https://raw.githubusercontent.com/OpenEnergyPlatform/open-MaStR/refs/heads/production/docs/images/README_HeaderThreePartners.svg
    :align: left
    :target: https://github.com/OpenEnergyPlatform/open-MaStR
    :alt: MaStR logo

==========
open-mastr
==========

**A package that provides an interface for downloading and processing the Marktstammdatenregister (MaStR)**

.. list-table::
   :widths: 10, 50

   * - License
     - |badge_license|
   * - Documentation
     - |badge_rtd|
   * - Tests
     - |badge_ci|
   * - Publication
     - |badge_pypi| |badge_joss|
   * - Data Publication
     - |badge_zenodo|
   * - Development
     - |badge_issue_open| |badge_issue_closes| |badge_pr_open| |badge_pr_closes|
   * - Community
     - |badge_contributing| |PyPI download month| |Total PyPI downloads|
   

.. contents::
    :depth: 2
    :local:
    :backlinks: top

Introduction
============

The python package ``open-mastr`` provides an interface for accessing the `Marktstammdatenregister (MaStR) <https://www.marktstammdatenregister.de/MaStR>`_ data. The MaStR is a German register provided by the German Federal Network Agency (Bundesnetzagentur / BNetza) that keeps track of all power and gas units located in Germany. It is a daily growing dataset with millions of data points covering electricity and gas production units, electricity and gas consumers, storages, grids, and energy market participants.

Generally, and besides the offerings of ``open-mastr``, the MaStR data can be accessed via three main options:

#. browse, filter and download `in the browser <https://www.marktstammdatenregister.de/MaStR>`_
#. download `daily provided dumps <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_
#. access via the `web service <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html>`_

These access options, however, are not exactly frictionless. ``open-mastr`` thus provides an interface for and improved developer experience of accessing the data. This project is intended for individuals who wish to "just work" with the MaStR data and who do not want to deal with the idiosyncrasies of the three access options above.

In particular, ``open-mastr`` facilitates access to the daily provided MaStR dumps with download methods (bulk) and by parsing the XML files to a relational database. Furthermore, the software provides a Python wrapper to access the MaStR SOAP web service (API).


**Does open-mastr edit or change the MaStR data?**
No. ``open-mastr`` is a wrapper around the MaStR data and does not edit or change the data. It is intended to be used as a tool for working with the MaStR data.

Benefits provided by ``open-mastr``
===================================

.. list-table::
   :widths: 30, 70
   :header-rows: 1

   * - Benefit
     - Description
   * - Data download and parsing
     - Download, decode, and write data to a local database
   * - Translation to English
     - Translate table names and columns from German to English as well as an English documentation page of the dataset
   * - Data processing
     - Merge relevant information about different technologies to single csv files

**Just here for the data?**
We regularly run the whole download and cleansing pipeline and upload the dataset as csv files at `zenodo <https://doi.org/10.5281/zenodo.6807425>`_!


Documentation
=============

| The documentation is in `Material for Mkdocs <https://squidfunk.github.io/mkdocs-material/>`_ markdown format in the ``doc`` sub-folder of the repository.
| Find the `documentation <https://open-mastr.readthedocs.io/en/latest/>`_ hosted on ReadTheDocs.

| The original API documentation can be found on the `Webhilfe des Marktstammdatenregisters <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html>`_.


Installation
============

| It is recommended to use a virtual python environment, for example `conda <https://docs.conda.io/en/latest/miniconda.html>`_ or `virtualenv <https://virtualenv.pypa.io/en/latest/installation.html>`_.
| The package is intended to be used with ``Python >=3.8``.


PyPI
----

Install the current release of ``open-mastr`` with ``pip``:

.. code-block:: python

    pip install open-mastr

GitHub
------

For development, clone this repository manually.

.. code-block:: python

    git clone git@github.com:OpenEnergyPlatform/open-MaStR.git
    cd open-MaStR

Setup the conda environment with

.. code-block:: python

    conda env create -f environment.yml

Install the package with

.. code-block:: python

    pip install "open_mastr[dev]"


Examples of Usage
==================
These projects already use open-mastr:

- `PV- und Windflächenrechner <https://www.agora-energiewende.de/service/pv-und-windflaechenrechner/>`_
- `Wasserstoffatlas <https://wasserstoffatlas.de/>`_
- `EE-Status App <https://ee-status.de/>`_
- `Digiplan Anhalt <https://digiplan.rl-institut.de/>`_
- `EmPowerPlan <https://epp.rl-institut.de/>`_
- `Goal100 Monitor <https://goal100.org/monitor>`_

If you want to see your project in this list, write an  
`Issue <https://github.com/OpenEnergyPlatform/open-MaStR/issues>`_ or add
changes in a `Pull Request <https://github.com/OpenEnergyPlatform/open-MaStR/pulls>`_.

External Resources
===================
Besides open-mastr, some other resources exist that ease the process of working with the Marktstammdatenregister:

- The `bundesAPI/Marktstammdaten-API <https://github.com/bundesAPI/marktstammdaten-api>`_ is another implementation to access data via an official API.

Collaboration
=============
| Everyone is invited to develop this repository with good intentions.
| Please follow the workflow described in the `CONTRIBUTING.md <https://github.com/OpenEnergyPlatform/open-MaStR/blob/production/CONTRIBUTING.md>`_.


License and Citation
====================

Software
--------

| This repository is licensed under the **GNU Affero General Public License v3.0 or later** (AGPL-3.0-or-later).
| See `LICENSE.md <https://github.com/OpenEnergyPlatform/open-MaStR/blob/production/LICENSE.md>`_ for rights and obligations.
| See the *Cite this repository* function or `CITATION.cff <https://github.com/OpenEnergyPlatform/open-MaStR/blob/production/CITATION.cff>`_ for citation of this repository.
| Copyright: `open-MaStR <https://github.com/OpenEnergyPlatform/open-MaStR/>`_ © `Reiner Lemoine Institut <https://reiner-lemoine-institut.de/>`_ © `fortiss <https://www.fortiss.org/>`_ © `OFFIS <https://www.offis.de/>`_  | `AGPL-3.0-or-later <https://www.gnu.org/licenses/agpl-3.0.txt>`_

Data
----
| The original dataset is licensed under the **Datenlizenz Deutschland – Namensnennung – Version 2.0** (DL-DE-BY-2.0)
| Copyright: `Marktstammdatenregister <https://www.marktstammdatenregister.de/MaStR>`_ - © Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen | `DL-DE-BY-2.0 <https://www.govdata.de/dl-de/by-2-0>`_


.. |badge_license| image:: https://img.shields.io/github/license/OpenEnergyPlatform/open-MaStR
    :target: LICENSE.md
    :alt: License

.. |badge_rtd| image:: https://readthedocs.org/projects/open-mastr/badge/?style=flat
    :target: https://open-mastr.readthedocs.io/en/latest/
    :alt: Read the Docs

.. |badge_ci| image:: https://github.com/OpenEnergyPlatform/open-MaStR/actions/workflows/ci-production.yml/badge.svg
    :target: https://github.com/OpenEnergyPlatform/open-MaStR/actions?query=workflow%3ACI
    :alt: GitHub Actions

.. |badge_pypi| image:: https://img.shields.io/pypi/v/open-mastr.svg
    :target: https://pypi.org/project/open-mastr/
    :alt: PyPI

.. |badge_zenodo| image:: https://zenodo.org/badge/DOI/10.5281/zenodo.6807426.svg
    :target: https://doi.org/10.5281/zenodo.6807425
    :alt: zenodo

.. |badge_issue_open| image:: https://img.shields.io/github/issues-raw/OpenEnergyPlatform/open-MaStR
    :alt: open issues

.. |badge_issue_closes| image:: https://img.shields.io/github/issues-closed-raw/OpenEnergyPlatform/open-MaStR
    :alt: closes issues

.. |badge_pr_open| image:: https://img.shields.io/github/issues-pr-raw/OpenEnergyPlatform/open-MaStR
    :alt: closes issues

.. |badge_pr_closes| image:: https://img.shields.io/github/issues-pr-closed-raw/OpenEnergyPlatform/open-MaStR
    :alt: closes issues

.. |badge_contributing| image:: https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat
    :alt: contributions
    
.. |PyPI download month| image:: https://img.shields.io/pypi/dm/open-mastr?label=PyPi%20Downloads
    :target: https://pypistats.org/packages/open-mastr

.. |Total PyPI downloads| image:: https://static.pepy.tech/badge/open-mastr
    :target: https://pepy.tech/project/open-mastr

.. |badge_joss| image:: https://joss.theoj.org/papers/dc0d33e7dc74f7233e15a7b6fe0c7a3e/status.svg
    :target: https://joss.theoj.org/papers/dc0d33e7dc74f7233e15a7b6fe0c7a3e


