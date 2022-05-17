[![Documentation Status](https://readthedocs.org/projects/open-mastr/badge/?version=dev)](https://open-mastr.readthedocs.io/en/dev/?badge=latest)
[![CI](https://github.com/OpenEnergyPlatform/open-MaStR/workflows/CI/badge.svg)](https://github.com/OpenEnergyPlatform/open-MaStR/actions?query=workflow%3ACI)
<a href="https://openenergyplatform.org"><img align="right" width="200" height="200" src="https://avatars2.githubusercontent.com/u/37101913?s=400&u=9b593cfdb6048a05ea6e72d333169a65e7c922be&v=4" alt="OpenEnergyPlatform"></a>

# open-mastr

The [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) is a German register 
provided by the German Federal Network Agency (Bundesnetzagentur) that keeps track of all power and gas units located in Germany.


The MaStR data can be 
1. browsed [online](https://www.marktstammdatenregister.de/MaStR)
2. taken from [daily provided dumps](https://www.marktstammdatenregister.de/MaStR/Datendownload)
3. be accessed via the [web service](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html)

The python package ``open-mastr`` provides an interface for accessing the data. 
It contains methods to download and parse the xml files (bulk) and the web service (API).

In this repository we are developing methods to analyze, validate and enrich the data.
We want to collect and compile post processing scripts to improve data quality.

If you are interested in browsing the MaStR online, check out the
privately hosted [Marktstammdatenregister.dev](https://marktstammdatenregister.dev/).

Find the [documentation](https://open-mastr.readthedocs.io/en/dev) hosted on ReadTheDocs.


## Installation

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

## License / Copyright

This repository is licensed under [GNU Affero General Public License v3.0 (AGPL-3.0)](https://www.gnu.org/licenses/agpl-3.0.en.html). <br>
See the [LICENSE](LICENSE.md) file for rights and limitations. <br>
See the CITATION function if you want to mention this software in your publication. 

[open-MaStR](https://github.com/OpenEnergyPlatform/open-MaStR) © [Reiner Lemoine Institut](https://reiner-lemoine-institut.de/) © [fortiss](https://www.fortiss.org/) | [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html)


