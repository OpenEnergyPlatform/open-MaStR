[![Documentation Status](https://readthedocs.org/projects/open-mastr/badge/?version=latest)](https://open-mastr.readthedocs.io/en/latest/?badge=latest)
[![CI](https://github.com/OpenEnergyPlatform/open-MaStR/workflows/CI/badge.svg)](https://github.com/OpenEnergyPlatform/open-MaStR/actions?query=workflow%3ACI)
<a href="https://openenergyplatform.org"><img align="right" width="200" height="200" src="https://avatars2.githubusercontent.com/u/37101913?s=400&u=9b593cfdb6048a05ea6e72d333169a65e7c922be&v=4" alt="OpenEnergyPlatform"></a>

# Open Energy Family - open_MaStR

Code to download and process German energy data from BNetzA database [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR).
Find the [documentation](https://open-mastr.readthedocs.io/en/latest) hosted of ReadTheDocs.
## License / Copyright

This repository is licensed under [GNU Affero General Public License v3.0 (AGPL-3.0)](https://www.gnu.org/licenses/agpl-3.0.en.html)

Please cite as:
_[open_MaStR](https://github.com/OpenEnergyPlatform/open-MaStR)_ © [Reiner Lemoine Institut](https://reiner-lemoine-institut.de/) | [AGPL-3.0](https://github.com/OpenEnergyPlatform/open-MaStR/blob/master/LICENSE)

## Installation

The package is intended to be used with Python >=3.6

- with conda

    ```
    conda env create -f environment.yml
   ```
   
   ```
    python setup.py install
   ```

- without conda

    In a virtual environment run `setup.py`.

    ```
    python setup.py install
   ```

# Using open-MaStR

Tools for downloading data from MaStR data base, data cleansing and analysis are provided

## Data download

The MaStR data can be [browsed online](https://www.marktstammdatenregister.de/MaStR), 
taken from [irregularly provided dumps](https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen/DatenaustauschundMonitoring/Marktstammdatenregister/MaStR_node.html) 
or be access via the [web service](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html).
Here, we aim for the latter. The SOAP API of the MaStR web service is wrapped by some python to ease downloading of data.

### Low-level download

`MaStRAPI()` binds SOAP queries as methods and automatically passes credentials to these. After 
instantiating the class with your credentials 

    from open_mastr.soap_api.sessions import MaStRAPI
    
    mastr_api = MaStRAPI(
        user="SOM123456789012",
        key=""koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies..."
    )

or calling it blank (asks for credentials on the first time and saves them safely)

    mastr_api = MaStRAPI() 

you're able to query data from MaStR, for example with

    mastr_api.GetListeAlleEinheiten(limit=2)
    
See the [MaStR reference](https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Funktionen_MaStR_Webdienste_V1.2.26.html) for all available queries.

### Bulk download

Set the version number in `utils.py`.

```
python open_mastr/soap_api/utils.py
```

### Technologies 

You can select the technologies and run the download code in `main.py`.

```
python open_mastr/soap_api/main.py
```

## Tests

To run the tests install the required packages

```
pip install -r requirements.txt

pip install -r tests/test_requirements.txt
```
Then execute the tests

```
pytest tests
```
