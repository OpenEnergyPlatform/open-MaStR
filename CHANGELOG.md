# Changelog
All notable changes to this project will be documented in this file.

The format is inpired from [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and the versiong aim to respect [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

Here is a template for new release sections

```
## [_._._] - 20XX-MM-DD

### Added
-
### Changed
-
### Removed
-
```

## [Unreleased]

### Added

- Introduce project home `~/.open-MaStR/config/` [#120](https://github.com/OpenEnergyPlatform/open-MaStR/issues/120)
- Documentation of post-processing [#117](https://github.com/OpenEnergyPlatform/open-MaStR/issues/117)
- Local execution of post-processing now possible, optionally in dockered database [#116](https://github.com/OpenEnergyPlatform/open-MaStR/issues/116)
- Introduce a python wrapper for 
  [MaStRAPI](https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Funktionen_MaStR_Webdienste_V1.2.26.html) 
  for low-level access [#83](https://github.com/OpenEnergyPlatform/open-MaStR/issues/83)
- Configure Travis-CI [#132](https://github.com/OpenEnergyPlatform/open-MaStR/issues/132)

### Changed

- configuration through config filen in `~/.open-MaStR/config/` [#120](https://github.com/OpenEnergyPlatform/open-MaStR/issues/120)
- move code into one package named `open_mastr` [#123](https://github.com/OpenEnergyPlatform/open-MaStR/issues/123)


### Removed


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

