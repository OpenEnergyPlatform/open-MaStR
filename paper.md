---
title: 'open-mastr: A Python Package to Download and Clean the German Energy Registry Marktstammdatenregister'
tags:
  - Python
  - energy
  - dataset
  - Germany
  - energy system
authors:
  - name: Florian Kotthoff
    orcid: 0000-0003-3666-6122
    affiliation: 1 # (Multiple affiliations must be quoted)
  - name: Christoph Muschner
    orcid: 0000-0001-8144-5260
    affiliation: 2
  - name: Deniz Tepe
    affiliation: 1
    orcid: 0000-0002-7605-0173
  - name: Guido Pleßmann
    affiliation: 2
    orcid: 
  - name: Ludwig Hülk
    orcid: 0000-0003-4655-2321
    corresponding: true # (This is how to denote the corresponding author)
    affiliation: 2
affiliations:
 - name: fortiss, Research Institute of the Free State of Bavaria, Germany
   index: 1
 - name: Reiner Lemoine Institut gGmbH, Germany
   index: 2
date: 01 August 2023
bibliography: paper.bib
#This paper compiles with the following command: docker run --rm --volume $PWD/paper:/data --user $(id -u):$(id -g) --env JOURNAL=joss openjournals/inara
#Here $PWD/paper is the folder where the paper.md file lies
---

# Summary
The python package `open-mastr` provides an interface for accessing and cleaning the German Energy Unit dataset called *Marktstammdatenregister* (MaStR).
`open-mastr` provides the possibility to build and update a local database of the entire registry, as well as to process the data for further evaluation.
Ultimately, the package provides methods to reduce the registry's parsing and processing time and thus enables energy system researchers to access and start working with the entire dataset right away.

# Statement of need
`open-mastr` was built to facilitate the process of downloading, parsing, and cleaning the Marktstammdatenregister dataset.
The MaStR is a German registry provided by the German Federal Network Agency (Bundesnetzagentur / BNetzA) [@Bundesnetzagentur2019_Marktstammdatenregister].
It was first published in 2019 and includes detailed information about more than 8.2 million electricity and gas production units, electricity and gas consumers, storages, grids, and energy market actors.
@Tepe2023_MaStR found 54 papers in the fields of sustainability studies, energy politics, energy data, energy system analysis, and energy economics that applied the MaStR dataset in their research.

Besides its relevance in research, the MaStR dataset provided by BNetzA bears some obstacles: 
First, the documentation of the data model and download methods are only provided in German. 
Second, many entries in the dataset are encoded. 
And finally, information that belongs together is distributed over several tables.
The python package `open-mastr` addresses those issues by translating the data model to English, decoding dataset information and the sensible restructuring of information in tables.


\textbf{Table 1. Summary of benefits provided by `open-mastr`}

Benefit | Description 
------- | ------ 
Data download and parsing | Download, decode, and write data to a local database 
 |
Translation to english | Translate table names and columns from German to English as well as an english documentation page of the dataset 
 |
Data processing | Merge relevant information about different technologies to single csv files

Besides `open-mastr`, no other software solutions exists that provides an interface to download and clean the MaStR dataset.
For other energy-related data, similar solutions exist: The _iotools_ module from `pvlib` implements access to different raw data sources via its _get_ methods [@Holmgren2018_pvlib]. 
Websites offer another approach to provide cleansed datasets : This is done by the Open Energy Platform [@Hulk2022_OpenEnergyFamily], the Open Power System Data [@Wiese2019_openPSD], the Global Power Plant Database [@byers2018_global], or the Public Utility Data Liberation Project [@Selvans2020_pudl].
The advantage of aforementioned web platforms is their simplicity in accessing data for end users, as users can simply download files in standardised formats, such as csv.
The disadvantage is, that end users have to rely on platform maintainers for data relevance and correctness.
Here, `open-mastr` comes at hand, and provides direct access for end users to the original data ensuring their recency and relevance.  


# Package description
The first and main use-case of the `open-mastr` package is to download and parse the MaStR registry to a local database.
The `open_mastr.Mastr` class and its download methods are used to achieve this. 
When running the _download_ method, the whole MaStR registry is downloaded from the MaStR website as zipped xml files. 
The downloaded xml files are extracted and parsed to a sqlite database.
The result is a local database of the MaStR. However, many textual data points are still encoded by IDs. 
Thus, as a last step, the `Mastr.download` method decodes the IDs to their actual meaning.

The local database is then ready for further processing.
Its columns can be translated to English with the `Mastr.translate` method.
Relevant information about different technologies, like wind turbines or PV systems, can be merged from multiple tables and written to csv using the method `Mastr.to_csv`.

A second use-case is the wrapper for the MaStR SOAP API. 
Calling the SOAP API directly can be interesting for specific users who do not need to download the whole registry.
All possible API requests, as described in the [official documentation](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html) are callable as methods of an `soap_api.download.MaStRAPI` object.
The classes `soap_api.download.MaStRDownload` and `soap_api.mirror.MaStRMirror` use the API to download some tables or the whole registry. 
Both classes offer very similar functionalities to the basic `Mastr.download` function with the differences, that they require an API key and daily API request limit exists.
Hence, the use-cases of the `MaStRDownload` and `MaStRMirror` are limited since BNetzA offers a way to download the whole registry as zipped xml files, as it is used by `Mastr.download`.

As an extra service for people that are not familiar with python, the developers offer the cleaned and reduced dataset created with `Mastr.to_csv` on Zenodo [@hulk2022_mastr_zenodo]. 

# Conclusion
In summary, `open-mastr` gathers community developed code to work with the Marktstammdatenregister.
It simplifies the data parsing and cleaning process for working on the German Energy System.
In the future, a steady maintenance of the python package is needed to handle changes in the dataset and its data model introduced by BNetzA. 
It is also planned to enhance MaStR's metadata in the future to further comply with FAIR data standards. 


# CRediT Authorship Statement
FK: Writing original draft, creating code for bulk download, writing documentation page
CM: Creating and maintaining code for API download, Review of draft, writing documentation page
DT: Maintaining code for API, bulk download, and csv export
GP: Creating code for API download
LH: Creating code for API download, Review of draft

# Acknowledgements
FK and DT acknowledge support by "Bayerische Staatsministerium für Wirtschaft, Landesentwicklung und Energie" as part of "Bayerischen Verbundförderprogramms (BayVFP) – Förderlinie Digitalisierung – Förderbereich Informations- und Kommunikationstechnik".

# References