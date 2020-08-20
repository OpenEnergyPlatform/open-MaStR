<a href="https://openenergyplatform.org"><img align="right" width="200" height="200" src="https://avatars2.githubusercontent.com/u/37101913?s=400&u=9b593cfdb6048a05ea6e72d333169a65e7c922be&v=4" alt="OpenEnergyPlatform"></a>

# Open Energy Family - open_MaStR

Code to download and process German energy data from BNetzA database [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR)

## License / Copyright

This repository is licensed under [GNU Affero General Public License v3.0 (AGPL-3.0)](https://www.gnu.org/licenses/agpl-3.0.en.html)

Please cite as:
_[open_MaStR](https://github.com/OpenEnergyPlatform/open-MaStR)_ © [Reiner Lemoine Institut](https://reiner-lemoine-institut.de/) | [AGPL-3.0](https://github.com/OpenEnergyPlatform/open-MaStR/blob/master/LICENSE)

## Installation

The package is intended to be used for python 3.6

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

## Download

### User config
In order to connect to the MastR SOAP API you need a `user` name and a `token`. <br>
The first time you run the code you will be prompted for that information and a `config.ini` file will be generated automatically for you.
Handle this file carefully.

The `config.ini` file at the repository's root level should have the following structure:
```
[MaStR]
token = <your token>
user = <your user name>
```

### Data version

Create a new branch named `data-release/x.x.x`

Set the data version number in `utils.py`.

```
python soap_api/utils.py
```

### Technologies 

You can select the technologies and run the download code in `main.py`.

```
python soap_api/main.py
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
