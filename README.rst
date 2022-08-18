
==========
open-mastr
==========

**A package that provides an interface for downloading and processing the Marktstammdatenregister (MaStR)**

.. list-table::
   :widths: auto

   * - License
     - |badge_license|
   * - Documentation
     - |badge_rtd|
   * - Tests
     - |badge_ci|
   * - Publication
     - |badge_pypi| |badge_zenodo|
   * - Development
     - |badge_issue_open| |badge_issue_closes| |badge_pr_open| |badge_pr_closes|
   * - Community
     - |badge_contributing| |badge_contributors| |badge_repo_counts|

.. contents::
    :depth: 2
    :local:
    :backlinks: top

Introduction
============

The `Marktstammdatenregister (MaStR) <https://www.marktstammdatenregister.de/MaStR>`_ is a German register 
provided by the German Federal Network Agency (Bundesnetzagentur / BNetza) that keeps track of all power and gas units located in Germany.

The MaStR data can be 
# 1. browsed `online <https://www.marktstammdatenregister.de/MaStR>`_
# 2. taken from `daily provided XML dumps <https://www.marktstammdatenregister.de/MaStR/Datendownload>`_
# 3. be accessed via the `web service <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html>`_

| The python package `open-mastr` provides an interface for accessing the data. 
| It contains methods to download and parse the xml files (bulk) and the web service (API).
| In this repository we are developing methods to analyze, validate and enrich the data.
| We want to collect and compile post processing scripts to improve data quality.


Documentation
=============

Documentation is in `sphinx
<http://www.sphinx-doc.org/en/stable/>`_ reStructuredText format 
in the `doc` sub-folder of the repository.
Find the [documentation](https://open-mastr.readthedocs.io/en/dev) hosted on ReadTheDocs.

If you are interested in browsing the MaStR online, check out the
privately hosted `Marktstammdatenregister.dev <https://marktstammdatenregister.dev/>`_.

Also see the `bundesAPI/Marktstammdaten-API <https://github.com/bundesAPI/marktstammdaten-api>`_ for another implementation.


Installation
============

It is recommended to use a virtual python environment, for example [conda](https://docs.conda.io/en/latest/miniconda.html) or 
[virtualenv](https://virtualenv.pypa.io/en/latest/installation.html).
The package is intended to be used with Python >=3.8

Install the current release of ``open-mastr`` with ``pip``:

    $ pip install open-mastr

To upgrade to a newer release use the ``--upgrade`` flag:

    $ pip install --upgrade open-mastr


Alternatively, you can manually download ``open-mastr`` from
`GitHub <https://github.com/OpenEnergyPlatform/open-MaStR>`_. 
Change the CWD to the download folder `open-MaStR/`.

```bash
git clone git@github.com:OpenEnergyPlatform/open-MaStR.git
cd open-MaStR
```

Optionally, a pre-defined conda environment can be used with 

```bash
conda env create -f environment.yml
```
   
Install the package with

```bash
python setup.py install
```


Collaboration
=============
| Everyone is invited to develop this repository with good intentions.
| Please follow the workflow described in the `CONTRIBUTING.md <CONTRIBUTING.md>`_.


License and Citation
====================
| The code of this repository is licensed under the **GNU Affero General Public License v3.0 or later** (AGPL-3.0-or-later).
| See `LICENSE.txt <LICENSE.txt>`_ for rights and obligations.
| See the *Cite this repository* function or `CITATION.cff <CITATION.cff>`_ for citation of this repository.
| Copyright: `open-MaStR <https://github.com/OpenEnergyPlatform/open-MaStR/>`_ © `Reiner Lemoine Institut <https://reiner-lemoine-institut.de/>`_ © `fortiss <https://www.fortiss.org/>`_  | `AGPL-3.0-or-later <https://www.gnu.org/licenses/agpl-3.0.txt>`_


.. |badge_license| image:: https://img.shields.io/github/license/OpenEnergyPlatform/open-MaStR
    :target: LICENSE.txt
    :alt: License

.. |badge_rtd| image:: https://readthedocs.org/projects/oemof-solph/badge/?style=flat
    :target: https://open-mastr.readthedocs.io/en/latest/
    :alt: Read the Docs

.. |badge_ci| image:: https://github.com/OpenEnergyPlatform/open-MaStR/workflows/CI/badge.svg
    :alt: GitHub Actions

.. |badge_pypi| image:: https://img.shields.io/pypi/v/open-mastr.svg
    :target: https://pypi.org/project/open-mastr/
    :alt: PyPI Package latest release

.. |badge_zenodo| image:: https://zenodo.org/badge/DOI/10.5281/zenodo.6807426.svg
    :target: https://doi.org/10.5281/zenodo.6807426
    :alt: Zenodo

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

.. |badge_repo_counts| image:: http://hits.dwyl.com/OpenEnergyPlatform/open-MaStR.svg
    :alt: counter
