
.. image:: https://user-images.githubusercontent.com/14353512/199113556-4b53660f-c628-4138-8d01-3719595ecda1.png
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
     - |badge_pypi|
   * - Data Publication
     - |badge_zenodo|
   * - Development
     - |badge_issue_open| |badge_issue_closes| |badge_pr_open| |badge_pr_closes|
   * - Community
     - |badge_contributing| |badge_contributors| |badge_repo_counts| |PyPI download month| |Total PyPI downloads|
   

.. contents::
    :depth: 2
    :local:
    :backlinks: top

Introduction
============

The `Marktstammdatenregister (MaStR) <https://www.marktstammdatenregister.de/MaStR>`_ is a German register 
provided by the German Federal Network Agency (Bundesnetzagentur / BNetza) that keeps track of all power and gas units located in Germany.

The MaStR data can be
 
#. browsed and filtered `online <https://www.marktstammdatenregister.de/MaStR>`_
#. taken from `daily provided dumps <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_
#. be accessed via the `web service <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html>`_

| The python package ``open-mastr`` provides an interface for accessing the data. 
| It contains methods to download and parse the xml files (bulk) and the SOAP web service (API).
| In this repository we are developing methods to analyze, validate and enrich the data.
| We want to collect and compile post processing scripts to improve data quality.


Documentation
=============

| The documentation is in `Material for Mkdocs <https://squidfunk.github.io/mkdocs-material/>`_ markdown format in the ``doc`` sub-folder of the repository.
| Find the `documentation <https://open-mastr.readthedocs.io/en/latest/>`_ hosted on ReadTheDocs.

| The original API documentation can be found on the `Webhilfe des Marktstammdatenregisters <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html>`_.
| If you are interested in browsing the MaStR online, check out the privately hosted `Marktstammdatenregister.dev <https://marktstammdatenregister.dev/>`_.
| Also see the `bundesAPI/Marktstammdaten-API <https://github.com/bundesAPI/marktstammdaten-api>`_ for another implementation.


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
If you want to see your project in this list, write an  
`Issue <https://github.com/OpenEnergyPlatform/open-MaStR/issues>`_ or add
changes in a `Pull Request <https://github.com/OpenEnergyPlatform/open-MaStR/pulls>`_.

- `PV- und Windflächenrechner <https://www.agora-energiewende.de/service/pv-und-windflaechenrechner/>`_
- `Wasserstoffatlas <https://wasserstoffatlas.de/>`_
- `EE-Status App <https://ee-status.de/>`_
- `Digiplan Anhalt <https://digiplan.rl-institut.de/>`_


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
| Copyright: `open-MaStR <https://github.com/OpenEnergyPlatform/open-MaStR/>`_ © `Reiner Lemoine Institut <https://reiner-lemoine-institut.de/>`_ © `fortiss <https://www.fortiss.org/>`_  | `AGPL-3.0-or-later <https://www.gnu.org/licenses/agpl-3.0.txt>`_

Data
----
| The data has the license **Datenlizenz Deutschland – Namensnennung – Version 2.0** (DL-DE-BY-2.0)
| Copyright: `Marktstammdatenregister <https://www.marktstammdatenregister.de/MaStR>`_ - © Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen | `DL-DE-BY-2.0 <https://www.govdata.de/dl-de/by-2-0>`_


.. |badge_license| image:: https://img.shields.io/github/license/OpenEnergyPlatform/open-MaStR
    :target: LICENSE.txt
    :alt: License

.. |badge_rtd| image:: https://readthedocs.org/projects/open-mastr/badge/?style=flat
    :target: https://open-mastr.readthedocs.io/en/latest/
    :alt: Read the Docs

.. |badge_ci| image:: https://github.com/OpenEnergyPlatform/open-MaStR/workflows/CI/badge.svg
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

.. |badge_contributors| image:: https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square
    :alt: contributors

.. |badge_repo_counts| image:: https://hits.dwyl.com/OpenEnergyPlatform/open-MaStR.svg
    :alt: counter
    
.. |PyPI download month| image:: https://img.shields.io/pypi/dm/open-mastr?label=PyPi%20Downloads
    :target: https://pypistats.org/packages/open-mastr

.. |Total PyPI downloads| image:: https://static.pepy.tech/badge/open-mastr
    :target: https://pepy.tech/project/open-mastr


