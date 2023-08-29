---
title: 'open-mastr: A Python package to download and clean the energy database Marktstammdatenregister'
tags:
  - Python
  - energy
  - dataset
  - Germany
  - energy system
authors:
  - name: Florian Kotthoff
    orcid: 0000-0000-0000-0000
    affiliation: 1 # (Multiple affiliations must be quoted)
  - name: Christoph Muschner
    affiliation: 2
  - name: Deniz Tepe
    affiliation: 1
  - name: Ludwig Hülk
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
- What is Marktstammdatenregister
- Two ways to get data 
- Download from both sources to the same database
- Basic data cleansing
- Processing to combined .csv files

The Marktstammdatenregister (MaStR) is a German register provided by the German Federal Network Agency (Bundesnetzagentur / BNetzA) [@Bundesnetzagentur2019_Marktstammdatenregister].
It was first published in 2019 and includes detailed information about more than 8.2 million electricity and gas producers, electricity and gas consumers, storages, grids, and actors from the energy market.
The python package open-mastr provides an interface for accessing and cleaning the dataset.
It can be used to build and update a local database of the whole registry.


# Statement of need
open-mastr was built to simplify the process of downloading, parsing, and cleaning the MaStR dataset.
@Tepe2023_MaStR found 54 papers in the fields of sustainability studies, energy politics, energy data, energy system ananlysis, and energy economics that used the MaStR dataset in their research.
The raw data format of the MaStR, as it is provided by BNetzA, is hard to handle: First, it can be retreived via two different methods, and both methods are only described in German. Second, many entries in the dataset are encoded. And finally, information that belongs together is distributed over several tables.
Hence, open-mastr is a tool that diminishes the time needed to parse and process data and instead enables researchers to start working with the data right away.

Besides open-mastr, no other software solutions exists that provides an interface to download and clean the MaStR dataset.
For other energy-related data, similar solutions exist: The iotools module from pvlib implements access to different raw data sources via its get methods [@Holmgren2018_pvlib]. 
Another existing approach is to offer cleansed datasets via the web: This is done by the Open Energy Platform [@Hulk2022_OpenEnergyFamily], the Open Power System Data [@Wiese2019_openPSD], the Global Power Plant Database [@byers2018_global], or the Public Utility Data Liberation Project [@Selvans2020_pudl].
The advantage of these web platforms is the easy access for end users, they can simply download files, often in easy to handle formats such as csv.
The disadvantage is, that the end users have to rely on the maintainers of the platforms to regularily update their files.
Here, open-mastr comes at hand, since end users can directly get the original data and hence do not depend on others to keep their data updated. 
# Package description
It contains methods to download and parse the data from the two different sources: the bulk download and the SOAP web service (API).
The cleaned data is then written to a SQL database.

**Take this from the finished documentation page**

# CRediT Authorship Statement
FK: Writing original draft, creating code for bulk download
CM: Creating code for API download, Review of draft
DT: Maintaining code for API, bulk download, and csv export
LH: Creating code for API download, Review of draft
# Acknowledgements
This material is supported by 

# References