# Release Procedure

The release procedure is a process in which different parts of the repository are involved.<br>
These symbols help with orientation:
* 🐙 GitHub
* 💠 git (Bash)
* 📝 File
* 💻 Command Line (CMD)
* 📦 Package (PyPI)


## Version Numbers

This software follows the [Semantic Versioning (SemVer](https://semver.org/).<br>
It always has the format `MAJOR.MINOR.PATCH`, e.g. `1.5.0`.

The data follows the [Calendar Versioning (CalVer)](https://calver.org/).<br>
It always has the format `YYYY-MM-DD`, e.g. `2022-05-16`.


## GitHub and PyPI Release

### 1. 🐙 Create a `GitHub Issue`
* Named `Release Patch v0.11.7`
* Use `📝ISSUE_TEMPLATE_RELEASE` (❗ToDo❗)
* Discuss a good and suitable name of the release
* Define a day for the release

▶️ This issue documents the status of the release!

### 2. 🐙 Create a `GitHub Project`
* Create [New classic project](https://github.com/OpenEnergyPlatform/open-MaStR/projects?type=classic)
* Use the project template *Automated kanban with reviews*
* Named `Release v0.11.7`
* Add a meaningful description
* Track project progress

▶️ It gives an overview of open and finished issues and pull requests!

### 3. 🐙 Create a `Draft GitHub Release`
* [Draft a new release](https://github.com/OpenEnergyPlatform/open-MaStR/releases/new)
* Enter the release version number `v0.11.7` as title
* Summarize key changes in the description
    * `## [v0.11.7] Patch - Name - Date`
    * `### Added`
    * `### Changed`
    * `### Removed`
* Add a link to compare versions
    * `**Compare versions:** [v0.11.6 - v0.11.7](https://github.com/OpenEnergyPlatform/open-MaStR/compare/v0.11.6...v0.11.7)`
* Add a link to the `📝CHANGELOG.md`
    * `Also see [**CHANGELOG.md**](https://github.com/OpenEnergyPlatform/open-MaStR/blob/production/CHANGELOG.md)`
* Save draft

### 4. 🐙 Finish all planned Developments
* Some days before the release, inform all developers
* Merge the open pull requests
* On release day, start the release early to ensure sufficient time for reviews
* Merge everything on the `develop` branch

### 5. 💠 Create a `release` branch
* Checkout `develop` and branch with `git checkout -b release-v0.11.7`
* Push branch with `git push --set-upstream origin release-v0.11.7`

### 6. 📝 Update the version files
* `📝CHANGELOG.md`
    * All Pull Request are included
    * Add a new section with correct version number
    * Give the suitable name to the release
* `📝CITATION.cff`
    * Update the version number
* `📝setup.py`
    * Update `version`
    * Update `download_url` (.../v0.11.7.tar.gz)

### 7. 🐙 Create a `Release Pull Request`
* Use `📝PR_TEMPLATE_RELEASE` (❗ToDo❗)
* Merge `release` into `production` branch
* Assign two reviewers to check the release
* Run all test
* Execute the software locally
* Wait for reviews and tests
* Merge PR but do not delete `release` branch

### 8. 💠 Set the `Git Tag`
* Checkout `production` branch and pull
* Check existing tags `git tag -n`
* Create new tag: `git tag -a v0.11.7 -m "super-repo Patch Release v0.11.7 with PyPI"`
* This commit will be the final version for the release, breath three times and check again
* Push tag: `git push --tags`
* If you messed up, remove tags and start again
    * Delete local tag: `git tag -d v0.11.7`
    * Delete remote tag: `git push --delete origin v0.11.7`

### 9. 💻 Create and publish package on PyPI
* Navigate to git folder `cd D:\git\github\GROUP\REPO\`
* Create package using `python setup.py sdist`
* Check that file has been created in folder `dist`
* Activate python environment `activate release_py38`
* Upload to PyPI using `twine upload dist/NAME_0.5.1.tar.gz`
* Enter `name` and `password`
* Check on PyPI if release arrived
* Breath three times and smile

▶️ Publish the Package 📦

### 10. 🐙 Publish `GitHub Release`
* Summarize key changes in the description
* Choose the correct git `tag`
* Choose the `production` branch
* Publish release

▶️ Release on GitHub!

### 11. 🐙 Set up new development
* Create a Pull request from `production` to `develop`
* Delete the `release` branch
* Create a new **unreleased section** in the `📝CHANGELOG.md`

▶️ Continue the developments 🛠

## Documentation on Read the Docs (RTD)
ToDo


## Sources:
* https://raw.githubusercontent.com/folio-org/stripes/master/doc/release-procedure.md
