[![Documentation Status](https://readthedocs.org/projects/open-mastr/badge/?version=dev)](https://open-mastr.readthedocs.io/en/dev/?badge=latest)
[![CI](https://github.com/OpenEnergyPlatform/open-MaStR/workflows/CI/badge.svg)](https://github.com/OpenEnergyPlatform/open-MaStR/actions?query=workflow%3ACI)
<a href="https://openenergyplatform.org"><img align="right" width="200" height="200" src="https://avatars2.githubusercontent.com/u/37101913?s=400&u=9b593cfdb6048a05ea6e72d333169a65e7c922be&v=4" alt="OpenEnergyPlatform"></a>

# open-MaStR

The [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) is a German register 
provided by the BNetzA keeping track of all power and gas units.


The MaStR data can be [browsed online](https://www.marktstammdatenregister.de/MaStR),
taken from [irregularly provided dumps](https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen/DatenaustauschundMonitoring/Marktstammdatenregister/MaStR_node.html)
or be access via the [web service](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html).
This package focuses on the latter.
It provides a python interface for accessing the web service API, methods to clean and enrich data and functions to
analyze it.

Find the [documentation](https://open-mastr.readthedocs.io/en/dev) hosted of ReadTheDocs.


## Installation

It it recommended to use a virtual python environment, for example [conda](https://docs.conda.io/en/latest/miniconda.html) or 
[virtualenv](https://virtualenv.pypa.io/en/latest/installation.html).
The package is intended to be used with Python >=3.6

Clone latest code from GitHub and change the CWD to the download folder `open-MaStR/`.

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

This repository is licensed under [GNU Affero General Public License v3.0 (AGPL-3.0)](https://www.gnu.org/licenses/agpl-3.0.en.html)
© [Reiner Lemoine Institut](https://reiner-lemoine-institut.de/).
See the [LICENSE](LICENSE.md) file for license rights and limitations.

