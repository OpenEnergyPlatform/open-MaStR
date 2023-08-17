# Introduction


The [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) is a German register 
provided by the German Federal Network Agency (Bundesnetzagentur / BNetza) that keeps track of all power and gas units located in Germany.

The MaStR data can be accessed via various options:
 
  1. browse, filter and download [online](https://www.marktstammdatenregister.de/MaStR)
  1. download [daily provided dumps](https://www.marktstammdatenregister.de/MaStR/Datendownload)
  1. access via the [web service](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html)

The python package `open-mastr` provides an interface for accessing the data. 
It contains methods to download and parse the xml files (bulk) and the SOAP web service (API).
In this repository we are developing methods to analyze, validate and enrich the data.
We want to collect and compile post processing scripts to improve data quality.

!!! question "Just here for the data?"
    :sparkles: We regularly run the whole download and cleansing pipeline and upload the dataset as csv files at [zenodo](https://doi.org/10.5281/zenodo.6807425)! 


## Contribution of open-mastr

MaStR-users face different barriers to access data depending on the access option above. <br>
open-mastr facilitates overcoming these barriers:


| use case | aim            | MaStR access option | MaStR barrier                                                  | MaStR data format | target data format                              | open-mastr support                               |
|----------|----------------|---------------------|----------------------------------------------------------------|-------------------|-------------------------------------------------|--------------------------------------------------|
| 1        | get MaStR data | 1                   | csv-chunks limited to 5000 data rows at a time                 | csv               | csv                                             | export all MaStR data to csv from local database |
|          |                | 2                   | data must be unpacked from XML                                 | XML               | ORM database (postgres-sql or sqlite or custom) | unpack XML files to ORM database                 |
|          |                | 3                   | 10000 API calls per day  develop code to access MaStR SOAP API |                   | ORM database (postgres-sql or sqlite or custom) | Python wrapper to access MaStR SOAP API          |


## License
The original dataset is licensed under the **Datenlizenz Deutschland – Namensnennung – Version 2.0** (DL-DE-BY-2.0)
[Marktstammdatenregister](https://www.marktstammdatenregister.de/MaStR) - © Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen | [DL-DE-BY-2.0](https://www.govdata.de/dl-de/by-2-0)