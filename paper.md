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
C:\Users\kotthoff\Documents\Projekte\eTwin\Code\github\open-mastr\open-MaStR\open_mastr
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
- No other software that handles Marktstammdatenregister, can save several days for individual user
- Data comes in hard to handle format (substituted words with numbers, )
- Two ways of receiving data combined in one package
- Merged information about technologies in .csv files
- State of the field: 

# Package description
It contains methods to download and parse the data from the two different sources: the bulk download and the SOAP web service (API).
The cleaned data is then written to a SQL database.


# Research and Future Development
- Used in previous paper
- 

# Citations

Citations to entries in paper.bib should be in
[rMarkdown](http://rmarkdown.rstudio.com/authoring_bibliographies_and_citations.html)
format.

If you want to cite a software repository URL (e.g. something on GitHub without a preferred
citation) then you can do it with the example BibTeX entry below for @fidgit.

For a quick reference, the following citation commands can be used:
- `@author:2001`  ->  "Author et al. (2001)"
- `[@author:2001]` -> "(Author et al., 2001)"
- `[@author1:2001; @author2:2001]` -> "(Author1 et al., 2001; Author2 et al., 2002)"

# Figures

Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Figure sizes can be customized by adding an optional second parameter:
![Caption for example figure.](figure.png){ width=20% }

# Acknowledgements



# References