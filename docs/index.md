# Introduction


The [Marktstammdatenregister (MaStR)](https://www.marktstammdatenregister.de/MaStR) is a German register 
provided by the German Federal Network Agency (Bundesnetzagentur / BNetza) that keeps track of all power and gas units located in Germany.

The MaStR data can be
 
  1. browsed and filtered [online](https://www.marktstammdatenregister.de/MaStR)
  1. taken from [daily provided dumps](https://www.marktstammdatenregister.de/MaStR/Datendownload)
  1. be accessed via the [web service](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html)

The python package `open-mastr` provides an interface for accessing the data. 
It contains methods to download and parse the xml files (bulk) and the SOAP web service (API).
In this repository we are developing methods to analyze, validate and enrich the data.
We want to collect and compile post processing scripts to improve data quality.

## License
The original dataset is licensed under the **Datenlizenz Deutschland – Namensnennung – Version 2.0** (DL-DE-BY-2.0)
[Marktstammdatenregister](https://www.marktstammdatenregister.de/MaStR) - © Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen | [DL-DE-BY-2.0](https://www.govdata.de/dl-de/by-2-0)