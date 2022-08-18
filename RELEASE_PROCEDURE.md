# Release Procedure

The release procedure is a process that invloves different parts of the repository.<br>
These symbols help to identify the place of interest:
* 🐙 GitHub
* 💠 git (Bash)
* 📝 File
* 💻 Command Line (CMD)
* 📦 Package (PyPi)


## Version Numbers

This software follows the [Semantic Versioning (SemVer](https://semver.org/).<br>
It always has the format `MAJOR.MINOR.PATCH`, e.g. `1.5.0`.

The data follows the [Calendar Versioning (CalVer)](https://calver.org/).<br>
It always has the format `YYYY-MM-DD`, e.g. `2022-05-16`.


## GitHub and PyPI Release

### 1. 🐙 Create a `GitHub Project`
* https://github.com/GROUP/REPO/projects?type=classic
* Use the project template *Automated kanban with reviews*
* Named `Release v0.5.1`
* Add a meaningful description
* Track project progress

▶️ It gives an overview of open and finished issues and pull requests!

### 2. 🐙 Create a `GitHub Issue`
* Named `Release Patch 0.5.1`
* Use `📝ISSUE_TEMPLATE_RELEASE` (❗ToDo❗)
* Discuss a good and suitable name of the release

▶️ This documents the status of the release!

### 3. 🐙 Finish all planned Developments
* Some days before the release, inform all developers
* Merge the open pull requests
* On release day, start the release early to ensure sufficient time for reviews
* Merge everything on the `develop` branch

### 4. 💠 Create a `release` branch
* Checkout `develop` and branch with `git checkout -b release-v0.5.1`
* Push branch with `git push --set-upstream origin release-v0.5.1`

### 5. 📝 Update the version files
* `📝CHANGELOG.md`
    * All Pull Request are included
    * Add a new section with correct version number
    * Give the suitable name to the release
* `📝CITATION.cff`
    * Update the version number
* `📝setup.py`
    * Update `version`
    * Update `download_url` (.../v0.5.1.tar.gz)
* `📝CI` (❗ToDo❗)
    * Update `download_url`

### 6. 🐙 Create a `Release Pull Request`
* Use `📝PR_TEMPLATE_RELEASE` (❗ToDo❗)
* Merge `release` into `production` branch
* Assign two reviewers to check the release
* Run all test
* Execute the software locally
* Wait for reviews and tests
* Merge PR but do not delete `release` branch

### 7. 💠 Set the `Git Tag`
* Checkout `production` branch and pull
* Check existing tags `git tag -n`
* Create new tag: `git tag -a v0.5.1 -m "super-repo Patch Release v0.5.1 with PyPi"`
* This commit will be the final version for the release, breath three times and check again
* Push tag: `git push --tags`
* If you messed up, remove tags and start again
    * Delete local tag: `git tag -d v0.5.1`
    * Delete remote tag: `git push --delete origin v0.5.1`

### 8. 💻 Create and publish package on PyPi
* Navigate to git folder `cd D:\git\github\GROUP\REPO\`
* Create package using `python setup.py sdist`
* Check that file has been created in folder `dist`
* Activate python environment `activate release_py38`
* Upload to PyPi using `twine upload dist/NAME_0.5.1.tar.gz`
* Enter *name* and *password*
* Check on PyPi if release arrived
* Breath three times and smile

▶️ Publish the Package 📦

### 9. 🐙 Create a `GitHub Release`
* https://github.com/GROUP/REPO/releases/new
* Choose the correct git *tag*
* Enter the version number as title
* Summarize key changes in the description
    * Add a link to the `📝CHANGELOG.md` section
        * `A comprehensive list of additions, changes and deletions can be found at [**CHANGELOG.md**](https://github.com/GROUP/REPO/blob/production/CHANGELOG.md)`
    * Add a link to compare versions
        * `**Compare versions:** [v0.5.0 - v0.5.1](https://github.com/OpenEnergyPlatform/open-MaStR/compare/v0.5.0...v0.5.1)`
* Save draft
* Publish release
    * ❓ There is a reason to first make a draft, but I don't know why (❗ToDo❗)
    * ❓ Move this section before the PyPi upload? (❗ToDo❗)

### 10. 🐙 Setup new release
* Create a Pull request from `production` to `develop`
* Create a new *unreleased section* in the `📝CHANGELOG.md`


## Documentation on Read the Docs (RTD)
ToDo


## Sources:
* https://raw.githubusercontent.com/folio-org/stripes/master/doc/release-procedure.md
