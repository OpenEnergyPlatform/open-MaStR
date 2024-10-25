# Release Procedure

The release procedure is a process in which different parts of the repository are involved.<br>
These symbols help with orientation:
* 🐙 GitHub
* 💠 git (Bash)
* 📝 File
* 💻 Command Line (CMD)
* 📦 Package


## Version Numbers

This software follows the [Semantic Versioning (SemVer)](https://semver.org/).<br>
It always has the format `MAJOR.MINOR.PATCH`, e.g. `1.5.0`.

The data follows the [Calendar Versioning (CalVer)](https://calver.org/).<br>
It always has the format `YYYY-MM-DD`, e.g. `2022-05-16`.


## GitHub and PyPI Release

### 1. 🐙 Create a `GitHub Issue`
* Named `Release Patch v0.12.1`
* Use `📝ISSUE_TEMPLATE_RELEASE` (❗ToDo❗)
* Discuss a good and suitable name of the release
* Define a day for the release

▶️ This issue documents the status of the release!

### 2. 🐙 Create a `GitHub Project`
* Create [New classic project](https://github.com/OpenEnergyPlatform/open-MaStR/projects?type=classic)
* Use the project template *Automated kanban with reviews*
* Named `Release v0.12.1`
* Add a meaningful description
* Track project progress

▶️ It gives an overview of open and finished issues and pull requests!

### 3. 🐙 Create a `Draft GitHub Release`
* [Draft a new release](https://github.com/OpenEnergyPlatform/open-MaStR/releases/new)
* Enter the release version number `v0.12.1` as title
* Save draft

### 4. 🐙 Finish all planned Developments
* Some days before the release, inform all developers
* Merge the open pull requests
* On release day, start the release early to ensure sufficient time for reviews
* Merge everything on the `develop` branch

### 5. Run tests and apply code linting
* Run tests locally with `pytest` and fix errors
* Apply linting with `pre-commit run -a` and fix errors

### 6. 💠 Create a `release` branch
* Checkout `develop` and branch with `git checkout -b release-v0.12.1`
* Update version for test release with `bump2version --current-version <current_version> --new-version <new_version> patch`
* Commit version update with `git commit -am "version update v0.12.1"`
* Push branch with `git push --set-upstream origin release-v0.12.1`

### 7. 📝 Update the version files
* `📝CHANGELOG.md`
    * All Pull Request are included
    * Add a new section with correct version number
    * Give the suitable name to the release
* `📝CITATION.cff`
    * Update `date-released`

### 8. Optional: Check release on Test-PyPI
* Check if the release it correctly displayed on [Test-PyPI](https://test.pypi.org/project/open-mastr/#history)
  * You can trigger the release manually within github actions using the `run workflow` button on branch `release-v0.12.1` on the workflow `Build and release on pypi tests`
  * Note: Pre-releases on Test-PyPI are only shown under `Release history` in the navigation bar.
  * Note: The branch status can only be released to a version on Test-PyPI once. Thus, for every branch status that you want to see on Test-PyPI increment the build version with `bump2version build` and push afterwards.
* Once testing on Test-PyPI is done, change the release version to the final desired version with `bump2version release`
  * Note: The release on Test-PyPI might fail, but it will be the correct release version for the PyPI server.
* Push commits to the `release-*` branch

### 9. 🐙 Create a `Release Pull Request`
* Use `📝PR_TEMPLATE_RELEASE` (❗ToDo❗)
* Merge `release` into `production` branch
* Assign reviewers to check the release
* Run all test
* Execute the software locally
* Wait for reviews and tests
* Merge PR

### 10. 💠 Set the `Git Tag`
* Checkout `production` branch and pull
* Check existing tags `git tag -n`
* Create new tag: `git tag -a v0.12.1 -m "open-mastr release v0.12.1 with PyPI"`
* This commit will be the final version for the release, breath three times and check again
* Push tag: `git push --tags`
* If you messed up, remove tags and start again
    * Delete local tag: `git tag -d v0.12.1`
    * Delete remote tag: `git push --delete origin v0.12.1`

### 11. 🐙 Publish `Release` on GitHub and PyPI
* Navigate to your [releases](https://github.com/OpenEnergyPlatform/open-MaStR/releases/) on GitHub and open your draft release.
* Summarize key changes in the description
    * Use the `generate release notes` button provided by github (This only works after the release branch is merged on production)
* Choose the correct git `tag`
* Choose the `production` branch
* Publish release

▶️ Release on GitHub!

▶️ In the background the GitHub workflow (pypi-publish.yml) will publish the package 📦 on PyPI!

### 12. 🐙 Set up new development
* Create a Pull request from `release-*` to `develop`
* Create a new **unreleased section** in the `📝CHANGELOG.md`
```
## [v0.XX.X] unreleased
### Added
### Changed
### Removed
```
* Merge `release-*` to `develop` and delete `release-*` branch

▶️ Continue the developments 🛠

## Documentation on Read the Docs (RTD)
* ReadTheDocs triggers a new built automatically after the release on github. To see
  the build status, visit https://readthedocs.org/projects/open-mastr/builds/


## Sources:
* https://raw.githubusercontent.com/folio-org/stripes/master/doc/release-procedure.md
