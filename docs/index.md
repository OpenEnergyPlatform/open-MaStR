# Introduction

The python package `open-mastr` provides an interface for accessing the [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) data. The MaStR is a German register provided by the German Federal Network Agency (Bundesnetzagentur / BNetza) that keeps track of all power and gas units located in Germany. It is a daily growing dataset with millions of data points covering electricity and gas production units, electricity and gas consumers, storages, grids, and energy market participants.

Generally, and besides the offerings of `open-mastr`, the MaStR data can be accessed via three main options:
 
  1. browse, filter and download [in the browser](https://www.marktstammdatenregister.de/MaStR)
  2. download [daily provided dumps](https://www.marktstammdatenregister.de/MaStR/Datendownload)
  3. access via the [web service](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html)

These access options, however, are not exactly frictionless. `open-mastr` thus provides an interface for and improved developer experience of accessing the data. This project is intended for individuals who wish to "just work" with the MaStR data and who do not want to deal with the idiosyncrasies of the three access options above.

In particular, `open-mastr` facilitates access to the daily provided MaStR dumps with download methods (bulk) and by parsing the XML files to a relational database. Furthermore, the software provides a Python wrapper to access the MaStR SOAP web service (API).

!!! info "Does `open-mastr` edit or change the MaStR data?"
    No. `open-mastr` is a wrapper around the MaStR data and does not edit or change the data. It is intended to be used as a tool for working with the MaStR data.

## Benefits provided by `open-mastr`

| Benefit                   | Description                                                                                                      |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Data download and parsing | Download, decode, and write data to a local database                                                             |
| Translation to English    | Translate table names and columns from German to English as well as an English documentation page of the dataset |
| Data processing           | Merge relevant information about different technologies to single csv files                                      |

!!! question "Just here for the data?"
    :sparkles: We regularly run the whole download and cleansing pipeline and upload the dataset as csv files at [zenodo](https://doi.org/10.5281/zenodo.6807425)! 


## License
The original dataset is licensed under the **Datenlizenz Deutschland – Namensnennung – Version 2.0** (DL-DE-BY-2.0)
[Marktstammdatenregister](https://www.marktstammdatenregister.de/MaStR) - © Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen | [DL-DE-BY-2.0](https://www.govdata.de/dl-de/by-2-0)