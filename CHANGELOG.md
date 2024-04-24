# Changelog

All notable changes to this project will be documented in this file.
For each version important additions, changes and removals are listed here. 

The format is inspired from [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and the versioning aims to respect [Semantic Versioning](http://semver.org/spec/v2.0.0.html).


## [v0.14.3] Fix Pypi Release - 2024-04-24 
### Added
- Add new table `changed_dso_assignment` [#510](https://github.com/OpenEnergyPlatform/open-MaStR/pull/510)
- Add deprecation warning for `MaStRMirror` and `MaStRDownload` [#492](https://github.com/OpenEnergyPlatform/open-MaStR/pull/492)
### Changed
- Change from `setup.py` to `pyproject.toml` based installation [#513](https://github.com/OpenEnergyPlatform/open-MaStR/pull/513)
### Removed
- Remove outdated and deprecated dependencies [#506](https://github.com/OpenEnergyPlatform/open-MaStR/pull/506)

## [v0.14.2] Maintenance - 2024-04-10
### Changed
- Fix and add URLs of example projects in readme [#481](https://github.com/OpenEnergyPlatform/open-MaStR/pull/481)
- No longer require web scraping for bulk download [#488](https://github.com/OpenEnergyPlatform/open-MaStR/pull/488)
- Replace deprecated pandas map function [#491](https://github.com/OpenEnergyPlatform/open-MaStR/pull/491)
- Fix the handling of corrupted xml syntax in the downloaded files [#494](https://github.com/OpenEnergyPlatform/open-MaStR/pull/494)
- Implement relevant API WSDL Patchnotes V24.1.128 [#499](https://github.com/OpenEnergyPlatform/open-MaStR/pull/499)
### Removed
- Remove unused Docker File [#501](https://github.com/OpenEnergyPlatform/open-MaStR/pull/501)


## [v0.14.1] Hotfix - 2024-01-17
### Changed
- Change data type of NetzbetreiberpruefungStatus to string [#483](https://github.com/OpenEnergyPlatform/open-MaStR/pull/483)

## [v0.14.0] Shiny New Documentation Page - 2023-11-22
### Added
- User-defined output path for csv, xml, database [#402](https://github.com/OpenEnergyPlatform/open-MaStR/pull/402)
- Add date=existing parameter to Mastr.download [#452](https://github.com/OpenEnergyPlatform/open-MaStR/pull/452)
- Update documentation and release it on mkdocs [#460](https://github.com/OpenEnergyPlatform/open-MaStR/pull/460)
- Replace values in ArtDerFlaecheIds with their entries from katalogwerte [#464](https://github.com/OpenEnergyPlatform/open-MaStR/pull/464)
- Add a Mastr.translate method for english translation [##471](https://github.com/OpenEnergyPlatform/open-MaStR/pull/471)

### Changed
- Using sphinx version <7 to build documentation [#454](https://github.com/OpenEnergyPlatform/open-MaStR/pull/454)
- Get missing column names in xml download in a robust way [#470](https://github.com/OpenEnergyPlatform/open-MaStR/pull/470)
- Adapt column names to the API web service update [#472](https://github.com/OpenEnergyPlatform/open-MaStR/pull/472)

### Removed
- Delete `on push` for github workflow [#445](https://github.com/OpenEnergyPlatform/open-MaStR/pull/445)

## [v0.13.2] Hotfix - 2023-08-07
### Changed
- Changed the name of the bulk tables for technology=gsgk [#456](https://github.com/OpenEnergyPlatform/open-MaStR/pull/456)

## [v0.13.1] Hotfix - 2023-04-11
### Added
- Add new table and new columns to the data model [#440](https://github.com/OpenEnergyPlatform/open-MaStR/pull/440)

## [v0.13.0] Maintenance release - 2023-02-16
### Added
- Add a `workflow_dispatch` to run CI pipelines from a button click [#389](https://github.com/OpenEnergyPlatform/open-MaStR/pull/389)
- Update version number with bump2version [#407](https://github.com/OpenEnergyPlatform/open-MaStR/pull/407)
- Add usage examples to Readme [#419](https://github.com/OpenEnergyPlatform/open-MaStR/pull/419)
- Check if the zipped file is corrupted [#422](https://github.com/OpenEnergyPlatform/open-MaStR/pull/422)
- Add Issue and PR Templates [#428](https://github.com/OpenEnergyPlatform/open-MaStR/pull/428)

### Changed
- Repair the csv export [#384](https://github.com/OpenEnergyPlatform/open-MaStR/pull/384)
- Refactor csv export for better maintainability [#401](https://github.com/OpenEnergyPlatform/open-MaStR/pull/401)
- Replace numeric value in hydro_extended [#392](https://github.com/OpenEnergyPlatform/open-MaStR/pull/392)
- Fix DB query errors when using SQLAlchemy v2.0 [#405](https://github.com/OpenEnergyPlatform/open-MaStR/pull/405)
- New columns introduced [#409](https://github.com/OpenEnergyPlatform/open-MaStR/pull/409)
- Replace numeric value in combustion [#413](https://github.com/OpenEnergyPlatform/open-MaStR/pull/413)
- Filenames.yml is created everytime Mastr class is called [#415](https://github.com/OpenEnergyPlatform/open-MaStR/pull/415)
- Omit connectionless DB query (SQLAlchemy v2.0 compatibility) [#421](https://github.com/OpenEnergyPlatform/open-MaStR/pull/421)

### Removed
- Option to filter & export by parameter StatistikFlag [#401](https://github.com/OpenEnergyPlatform/open-MaStR/pull/401)
- Removed unused package pynodo from dependencies [#424](https://github.com/OpenEnergyPlatform/open-MaStR/pull/424)
- Removed feature for dockered databases [#426](https://github.com/OpenEnergyPlatform/open-MaStR/pull/426)

## [v0.12.1] Patch release - 2022-11-15
### Added
- Add workflows to release on PyPI and Test-PyPI [#375](https://github.com/OpenEnergyPlatform/open-MaStR/pull/375)
### Changed
- Change error handling for missing tables in postgres [#370](https://github.com/OpenEnergyPlatform/open-MaStR/pull/370)


## [v0.12.0] Minor Release - 2022-11-07

### Added
- New table deleted_units [#360](https://github.com/OpenEnergyPlatform/open-MaStR/pull/360)
- Constants.py for central hardcoded variables [#345](https://github.com/OpenEnergyPlatform/open-MaStR/pull/345)

### Changed
- Handle case for empty list [#362](https://github.com/OpenEnergyPlatform/open-MaStR/pull/362)
- Fix bug occuring for permit and location download from the bulk download source [#358](https://github.com/OpenEnergyPlatform/open-MaStR/pull/358)
- Update documentation for readthedocs [#356](https://github.com/OpenEnergyPlatform/open-MaStR/pull/356)
- Update release procedure [#355](https://github.com/OpenEnergyPlatform/open-MaStR/pull/355)
- Update README.md to README.rst [#354](https://github.com/OpenEnergyPlatform/open-MaStR/pull/354)
- Technology parameter is renamed to data for better comprehension [#337](https://github.com/OpenEnergyPlatform/open-MaStR/pull/337)
- Update ORM and documentation according to wsdl patch V1.2.87 [#352](https://github.com/OpenEnergyPlatform/open-MaStR/pull/352)
- Date parameters are united into one parameter [#353](https://github.com/OpenEnergyPlatform/open-MaStR/pull/353)
- Dataversion folder entails export date[#347](https://github.com/OpenEnergyPlatform/open-MaStR/pull/347)

### Removed
- Removed code artefacts [#335](https://github.com/OpenEnergyPlatform/open-MaStR/pull/335)

## [v0.11.7] Patch - Hotfix - 2022-08-25
### Changed
- Fix dependencies in setup.py [#341](https://github.com/OpenEnergyPlatform/open-MaStR/pull/341)

## [v0.11.6] Patch - Hotfix - 2022-08-17
### Changed
- Repair technology paramter in API download [#327](https://github.com/OpenEnergyPlatform/open-MaStR/pull/327)


## [v0.11.5] Patch - Repair package - 2022-08-16

### Added
- Pre-commit hook for Black formatting [#312](https://github.com/OpenEnergyPlatform/open-MaStR/pull/312)

### Changed
- Use exclusively sqlalchemy for dialect-free implementation [#289](https://github.com/OpenEnergyPlatform/open-MaStR/pull/289)
- API Download: Linked units in table permit are written in comma seperated string [#302](https://github.com/OpenEnergyPlatform/open-MaStR/pull/302)
- API Download: Repaired Location download [#303](https://github.com/OpenEnergyPlatform/open-MaStR/pull/303)
- CSV export: Tables with mastr_data about power units are exported in chunks [#306](https://github.com/OpenEnergyPlatform/open-MaStR/pull/306)
- API Download: User credentials are now tested before the download starts [#309](https://github.com/OpenEnergyPlatform/open-MaStR/pull/309)

### Removed
- Delete the MastrDownloadFactory class [#292](https://github.com/OpenEnergyPlatform/open-MaStR/pull/292)


## [v0.11.4] Patch - Hotfix - 2022-07-08

### Added

### Changed
- Move function cleaned_data to data_io [#284](https://github.com/OpenEnergyPlatform/open-MaStR/pull/284)


## [v0.11.3] Patch - A data release - 2022-07-07

### Added
- Add warning message if the bulk download speed falls below a certain limit [#256](https://github.com/OpenEnergyPlatform/open-MaStR/issues/256)
- Add engine parameter for master class initialisation [#270](https://github.com/OpenEnergyPlatform/open-MaStR/pull/270)

### Changed
- Refactor code and restructure modules [#273](https://github.com/OpenEnergyPlatform/open-MaStR/pull/273)
- Fix combustion mapping [#253](https://github.com/rl-institut/super-repo/pull/253)
- Update bulk parsing order [#257](https://github.com/rl-institut/super-repo/pull/257)
- Apply csv method to all tables [#275](https://github.com/OpenEnergyPlatform/open-MaStR/pull/275)


## [v0.11.2] Patch - Patch the package - 2022-05-17

### Changed
- Update readme.md to improve PyPi release [#249](https://github.com/OpenEnergyPlatform/open-MaStR/issues/249)
- Rename branches `dev` -> `develop` and `master` -> `production`


## [v0.11.1] Patch - Make it a package - 2022-05-16

### Added
- Add files and metadata for PyPi release [#237](https://github.com/OpenEnergyPlatform/open-MaStR/issues/237)


##  [v0.11.0] Unreleased - Forces unite - 2022-05-16

The code becomes a python package and will be available on pypi.org
Additionally, a new datasource was identified and can be used: the xml bulk download.
The API was updated to the newest version and the data model was adapted.

### Added
- The class :class:`open_mastr.mastr.Matr` 
  was introduced as the entrypoint for users, the API download was included in this entrypoint 
  [#203](https://github.com/OpenEnergyPlatform/open-MaStR/issues/203)
- A method for downloading and parsing the xml dump from the MaStR website
  was implemented 
  [#202](https://github.com/OpenEnergyPlatform/open-MaStR/issues/202)
- New data classes and attributes were introduced to orm.py 
[#209](https://github.com/OpenEnergyPlatform/open-MaStR/issues/209)
- The documentation page was updated
- Unit tests were created 
[#207](https://github.com/OpenEnergyPlatform/open-MaStR/issues/207)
- A CI pipeline was introduced 
[#208](https://github.com/OpenEnergyPlatform/open-MaStR/issues/208)
- The metadata was updated 
[#219](https://github.com/OpenEnergyPlatform/open-MaStR/issues/219)


## [v0.10.0] Unreleased - Refactoring - 2020-10-08
A complete refactoring took place! Downloading data was entirely changed; introducing layers of code and removing 
duplicated code while more of less following DRY. 
Moreover, post-processing was changed to be more accessible and easier to execute. For example, docker now helps to 
spin up a database container.
The documention on RTD was extended, update and improved to be more helpful for new users.
Read more about the details:

### Added
- added more technologies
- added documentation for ReadTheDocs
- improved parallel download
- merged all stale branches
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
  and the missing LICENSE file for Zenodo is fixed in
  [#186](https://github.com/OpenEnergyPlatform/open-MaStR/issues/186)
- Add postgres database service in CI job for interacting with database in tests
  [#159](https://github.com/OpenEnergyPlatform/open-MaStR/issues/159)
- Tests for :class:`open_mastr.soap_api.mirror.MaStRMirror`
  [#191](https://github.com/OpenEnergyPlatform/open-MaStR/issues/191)
- Download functionality for Lokationen (with focus on :class:`open_mastr.soap_api.mirror.MaStRMirror`)
  [#162](https://github.com/OpenEnergyPlatform/open-MaStR/issues/162)
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
- Generalize CSV reading function 
  [#188](https://github.com/OpenEnergyPlatform/open-MaStR/issues/188)

### Removed
- Most of prior code for downloading data


## [v0.9.0] Initial Release - Try and Errors - 2019-12-05

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


## [v0.8.0] 2019-09-30

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
