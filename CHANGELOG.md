# Changelog

For each version important additions, changes and removals are listed here. 

## [Unreleased]

A complete refactoring took place! Downloading data was entirely changed; introducing layers of code and removing 
duplicated code while more of less following DRY. 
Moreover, post-processing was changed to be more accessible and easier to execute. For example, docker now helps to 
spin up a database container.
The documention on RTD was extended, update and improved to be more helpful for new users.
Read more about the details:

### Added

- The class :class:`open_mastr.soap_api.mirror.MaStRMirror` 
  was introduced for mirroring MaStR data with latest updates 
  [#149](https://github.com/OpenEnergyPlatform/open-MaStR/issues/149)
- Introduce project home `~/.open-MaStR/config/` [#120](https://github.com/OpenEnergyPlatform/open-MaStR/issues/120)
- Documentation of post-processing [#117](https://github.com/OpenEnergyPlatform/open-MaStR/issues/117)
- Updated documentation of downloading data
  [#124](https://github.com/OpenEnergyPlatform/open-MaStR/issues/124) which is harmonized with the other parts of docs
  and with GitHubs README [#135](https://github.com/OpenEnergyPlatform/open-MaStR/issues/135)
- Local execution of post-processing now possible, optionally in dockered database 
  [#116](https://github.com/OpenEnergyPlatform/open-MaStR/issues/116)
- Post-processing adapted to CSV data from :class:`open_mastr.soap_api.mirror.MaStRMirror`
  [#172](https://github.com/OpenEnergyPlatform/open-MaStR/issues/172)
- Tests for changed download code are added [#131](https://github.com/OpenEnergyPlatform/open-MaStR/issues/131)
- Metadata added for raw data as frictionless data package 
  [#160](https://github.com/OpenEnergyPlatform/open-MaStR/issues/160)
- Suffix columns instead of deferring in database CSV export
  [#157](https://github.com/OpenEnergyPlatform/open-MaStR/issues/157)
- Code examples added for :class:`open_mastr.soap_api.mirror.MaStRMirror` explaining basic use of 
  mirroring database [#164](https://github.com/OpenEnergyPlatform/open-MaStR/issues/164)
- CSV file reader for MaStR raw data added
  [#181](https://github.com/OpenEnergyPlatform/open-MaStR/issues/181)
- Zenodo data upload
  [#173](https://github.com/OpenEnergyPlatform/open-MaStR/issues/173)
- The CHANGELOG is now included in the documentation

### Changed

- Download of raw data has entirely been refactored. A 
  [python wrapper](https://open-mastr.readthedocs.io/en/latest/download.html#mastr-api-wrapper) for querying 
  the MaStR API was introduced
  [#83](https://github.com/OpenEnergyPlatform/open-MaStR/issues/83)
- Based on that, for bulk data download, 
  [MaStRDownload](https://open-mastr.readthedocs.io/en/latest/download.html#bulk-download) provides handy query 
  functions for power unit data
  [#86](https://github.com/OpenEnergyPlatform/open-MaStR/issues/86). See also
  [#128](https://github.com/OpenEnergyPlatform/open-MaStR/issues/128)
- configuration through config filen in `~/.open-MaStR/config/` with less hard-coded parameters in source files
  [#120](https://github.com/OpenEnergyPlatform/open-MaStR/issues/120), 
  [#112](https://github.com/OpenEnergyPlatform/open-MaStR/issues/112)
- move code into one package named `open_mastr` [#123](https://github.com/OpenEnergyPlatform/open-MaStR/issues/123)
- Switch to GitHub Actions for CI instead of Travis [#143](https://github.com/OpenEnergyPlatform/open-MaStR/issues/143)
- Fixed unexpected line breaks during CSV export that corrupted data 
  [#170](https://github.com/OpenEnergyPlatform/open-MaStR/issues/170)
- Filtering of duplicates in MaStR data (see 
  `MaStR help <https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html>`_) got changed to filter
  units by leading three characters and select only directly entered data
  [#180](https://github.com/OpenEnergyPlatform/open-MaStR/issues/180)


### Removed

- Most of prior code for downloading data


## [0.10.0] 2020-10-08

### Added
- added more technologies
- added documentation for ReadTheDocs
- improved parallel download
- merged all stale branches

## [0.9.0] 2019-12-05

### Added
- docstrings for functions
- tests
- setup.py file
- added update function (based on latest timestamp in powerunits csv)
- added wind functions 
  * only download power units for wind to avoid massive download
  * changed : process units wind ("one-click solution")
- added loop to retry failed power unit downloads, currently one retry
- write failed downloads to file

### Changed
- rename `import-api` `soap_api`
- update README with instruction for tests
- update README with instruction for setup

### Removed
- unused imports
- obsolete comments

### Fixed
- power unit update
- filter technologies from power units

## [0.8.0] 2019-09-30

### Added
- README.md
- CHANGELOG.md
- CONTRIBUTING.md
- LICENSE
- continuous integration with TravisCI (`.travis.yml`)
- linting tests and their config files (`.pylintrc` and `.flake8`)
- requirements.txt
- parallelized download for power units and solar
- utils.py for utility functions
- added storage units download
- added wind permit download
- ontology folder (#46)

### Changed
- took the code from this [repository's subfolder](https://github.com/OpenEnergyPlatform/data-preprocessing/tree/master/data-import/bnetza_mastr)

