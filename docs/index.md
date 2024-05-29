# Introduction

The [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) is a German register 
provided by the German Federal Network Agency (Bundesnetzagentur / BNetza) that keeps track of all power and gas units located in Germany.
It is a daily growing dataset with more than 8.2 million data points covering electricity and gas production units, electricity and gas consumers, storages, grids, and energy market participants (as of spring 2024).

Generally, the MaStR data can be accessed via various options:
 
  1. browse, filter and download [online](https://www.marktstammdatenregister.de/MaStR)
  1. download [daily provided dumps](https://www.marktstammdatenregister.de/MaStR/Datendownload)
  1. access via the [web service](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html)

The python package `open-mastr` provides an interface for accessing the data and contributes to improving the 
usability of the access options above. This repository is intended for people who wish to simply work with the 
MaStR data and do not want to deal with the individual obstacles to data access of the three options above.  <br> <br>
It facilitates access to the daily provided MaStR dumps with download methods (bulk) and by 
parsing the XML files to a relational database. Furthermore, the software provides a Python wrapper to access the MaStR 
SOAP web service (API).

## Benefits provided by `open-mastr`

Benefit | Description 
------- | ------ 
Data download and parsing | Download, decode, and write data to a local database 
Translation to English | Translate table names and columns from German to English as well as an English documentation page of the dataset 
Data processing | Merge relevant information about different technologies to single csv files

!!! question "Just here for the data?"
    :sparkles: We regularly run the whole download and cleansing pipeline and upload the dataset as csv files at [zenodo](https://doi.org/10.5281/zenodo.6807425)! 


## License
The original dataset is licensed under the **Datenlizenz Deutschland – Namensnennung – Version 2.0** (DL-DE-BY-2.0)
[Marktstammdatenregister](https://www.marktstammdatenregister.de/MaStR) - © Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen | [DL-DE-BY-2.0](https://www.govdata.de/dl-de/by-2-0)