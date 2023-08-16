---
title: 'open-mastr: A Python package to download and clean the energy database Marktstammdatenregister (MaStR)'
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
Due to its rich and diverse content, this dataset is a key component for research in the domains of energy research and sustainability studies in Germany [@Tepe2023_MaStR].
The original source data format however is hard to handle: First, it can be retreived via two different methods, and both methods are only described in German. Second, many entries in the dataset are encoded. And finally, information that belongs together is distributed over several tables.
Hence, open-mastr is a tool that diminishes the time needed to start actual research about the German energy system.

Besides open-mastr, no other software solutions exists that provides an interface to download and clean the MaStR dataset.
For other energy-related data, similar solutions exist: The iotools module from pvlib implements access to different raw data sources via its get methods [@Holmgren2018_pvlib]. 
# Package description
It contains methods to download and parse the data from the two different sources: the bulk download and the SOAP web service (API).
The cleaned data is then written to a SQL database.

**Take this from the finished documentation page**


# Research and Future Development
- Used in previous paper
- 

# Citations

# Figures

Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Figure sizes can be customized by adding an optional second parameter:
![Caption for example figure.](figure.png){ width=20% }

# Acknowledgements


# References