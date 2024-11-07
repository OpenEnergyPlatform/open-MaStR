---
name: Release Issue
about: For new version releases of open-mastr
title: Release Version vx.x.x
labels: "release"
assignees: ''

---
**Release Title**: *What is new in one sentence*
**Release Date**:



# Release Procedure
## Preparation
- [ ] 🐙 Create a `Draft GitHub Release` with the release version number `vx.x.x` as title
- [ ] Merge all open Pull Requests to `develop`
- [ ] Run tests locally with `pytest` and apply linting with `pre-commit run -a` 
## Create a `release` branch
- [ ] Checkout `develop` and branch with `git checkout -b release-vx.x.x`
- [ ] Update version for test release with `bump2version --current-version current_version> --new-version <new_version> patch`
- [ ] Update `date-released` in `📝CITATION.cff`
- [ ] Update `📝CHANGELOG.md`
    * All Pull Request are included
    * Add a new section with correct version number
    * Give the suitable name to the release
- [ ] Commit version update with `git commit -am "version update vx.x.x"`
## Optional: Check release on Test-PyPI
This should be done when the `README` file was changed, to assure a correct display of it at pypi.
- [ ] trigger the release manually within github actions using the `run workflow` button on branch `release-vx.x.x` on the workflow `Build and release on pypi tests`
  * Note: Pre-releases on Test-PyPI are only shown under `Release history` in the navigation bar.
  * Note: The branch status can only be released to a version on Test-PyPI once. Thus, for every branch status that you want to see on Test-PyPI increment the build version with `bump2version build` and push afterwards.
  * Check if the release it correctly displayed on [Test-PyPI](https://test.pypi.org/project/open-mastr/#history)
## 🐙 Create a `Release Pull Request` with the name `Release vx.x.x`
- [ ] Merge `release` into `production` branch
- [ ] Create new tag
  * Either by using the `Create new tag on release` method within the github draft Release
  * Or manually:
    * Checkout `production` branch and pull
    * checking existing tags `git tag -n` with using `git tag -a v0.12.1 -m "open-mastr release v0.12.1 with PyPI"` and push it with `git push --tags`
    * This commit will be the final version for the release, breath three times and check again

### 🐙 Publish `Release` on GitHub and PyPI
* Navigate to your [releases](https://github.com/OpenEnergyPlatform/open-MaStR/releases/) on GitHub and open your draft release.
- [ ] Summarize key changes in the description
  * Use the `generate release notes` button provided by github (This only works after the release branch is merged on production)
  * Choose the correct git `tag`
  * Choose the `production` branch
- [ ] Publish release

▶️ Release on GitHub!
▶️ In the background the GitHub workflow (pypi-publish.yml) will publish the package 📦 on PyPI!

### 🐙 Set up new development
- [ ] Create a Pull request from `release-*` to `develop`
- [ ] Create a new **unreleased section** in the `📝CHANGELOG.md`
        ```
        ## [v0.XX.X] unreleased
        ### Added
        ### Changed
        ### Removed
        ```
- [ ] Merge `release-*` to `develop` and delete `release-*` branch


## Documentation on Read the Docs (RTD)
* ReadTheDocs triggers a new built automatically after the release on github. To see
  the build status, visit https://readthedocs.org/projects/open-mastr/builds/

## Additional notes

The release procedure is a process in which different parts of the repository are involved.<br>
These symbols help with orientation:
* 🐙 GitHub
* 💠 git (Bash)
* 📝 File
* 💻 Command Line (CMD)
* 📦 Package

This software follows the [Semantic Versioning (SemVer)](https://semver.org/).
It always has the format `MAJOR.MINOR.PATCH`, e.g. `1.5.0`.
The data follows the [Calendar Versioning (CalVer)](https://calver.org/).
It always has the format `YYYY-MM-DD`, e.g. `2022-05-16`.

## Sources:
* https://raw.githubusercontent.com/folio-org/stripes/master/doc/release-procedure.md
