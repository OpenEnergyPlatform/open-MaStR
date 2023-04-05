<!--SPDX-License-Identifier: MIT-->
<!--Version: v1.0.0-->

# Collaborative Development

## Prerequisites
- [Git](https://git-scm.com/)
- [GitHub](https://github.com/)

## Types of interaction
This repository is following the [Contributor Covenant Code of Conduct](https://github.com/rl-institut/super-repo/blob/main/CODE_OF_CONDUCT.md). <br>
Please be self-reflective and always maintain a good culture of discussion and active participation.

### A. Use
Since the open license allows free use, no notification is required. 
However, for the authors it is valuable information who uses the software for what purpose. 
Indicators are `Watch`, `Fork` and `Starred` of the repository. 
If you are a user, please add your name and details in USERS.cff

### B. Comment
You can give ideas, hints or report bugs in issues, in PR, at meetings or other channels. 
This is no development but can be considered a notable contribution. 
If you wish, add your name and details to `CITATION.cff`.

### C. Contribute and Review
You add code and become an author of the repository. 
You must follow the workflow!

### D. Mantain and Release
You contribute and take care of the repository. 
You review and answer questions. 
You coordinate and carry out the release.

## Workflow
The workflow for contributing to this project has been inspired by the workflow described by [Vincent Driessen](https://nvie.com/posts/a-successful-git-branching-model/).

### 1. Describe the issue on GitHub
Create [an issue](https://help.github.com/en/articles/creating-an-issue)
in the GitHub repository. 
The `issue title` describes the problem you will address.  <br>
This is an important step as it forces one to think about the "issue".
Make a checklist for all needed steps if possible.

### 2. Solve the issue locally

#### 2.0. Get the latest version of the `develop` branch
Load the `develop branch`:
```bash
git checkout develop
```

Update with the latest version:
```bash
git pull
```

##### Permanent branches
* production - includes the current stable version
* develop - includes all current developments

#### 2.1. Create a new (local) branch
Create a new feature branch:
```bash
git checkout -b feature-1314-my-feature
```

Naming convention for branches: `type`-`issue-nr`-`short-description`

##### `type`
* feature - includes the feature that will be implemented
* hotfix - includes small improvements before an release, should be branched from a release branch
* release - includes the current version to be released

The majority of the development will be done in `feature` branches.

##### `issue-nr`
The `issueNumber` should be taken from Step 1. Do not use the "#". 

##### `short-description`
Describe shortly what the branch is about. 
Avoid long and short descriptive names for branches, 2-4 words are optimal.

##### Other hints
- Separate words with `-` (minus)
- Avoid using capital letters
- Do not put your name to the branch name, it's a collaborative project
- Branch names should be precise and informative

Examples of branch names: `feature-42-add-new-ontology-class`, `feature-911-branch-naming-convention`, `hotfix-404-update-api`, `release-v0.10.0`

#### 2.2. Start editing the files
- Divide your feature into small logical units
- Start to write the documentation or a docstring
- Don't rush, have the commit messages in mind
- Add your changes to the CHANGELOG.md

On first commit to the repo:
- Add your name and details to CITATION.cff

Check branch status:
```bash
git status
```

#### 2.3. Commit your changes 
If the file does not exist on the remote server yet, use:
```bash
git add filename.md
```

Then commit regularly with:
```bash
git commit filename.md
```

Write a good `commit message`:
- "If applied, this commit will ..."
- Follow [existing conventions for commit messages](https://chris.beams.io/posts/git-commit)
- Keep the subject line [shorter than 50 characters](https://chris.beams.io/posts/git-commit/#limit-50)
- Do not commit more than a few changes at the time: [atomic commits](https://en.wikipedia.org/wiki/Atomic_commit)
- Use [imperative](https://chris.beams.io/posts/git-commit/#imperative)
- Do not end the commit message with a [period](https://chris.beams.io/posts/git-commit/#end) ~~.~~ 
- Allways end the commit message with the `issueNumber` including the "#"

Examples of commit message: `Added function with some method #42` or `Update documentation for commit messages #1`

#### 2.4 Fix your latest commit message
Do you want to improve your latest commit message? <br>
Is your latest commit not pushed yet? <br>
Edit the commit message of your latest commit:
```bash
git commit --amend
```

### 3. Push your commits
Push your `local` branch on the remote server `origin`. <br>
If your branch does not exist on the remote server yet, use:
```bash
git push --set-upstream origin feature-1314-my-feature
```

Then push regularly with:
```bash
git push
```

### 4. Submit a pull request (PR)
Follow the GitHub guide [creating-a-pull-request](https://help.github.com/en/articles/creating-a-pull-request). <br>
The PR should be directed: `base: develop` <- `compare: feature-1-collaboration`. <br>
Add the line `Close #<issue-number>` in the description of your PR.
When it is merged, it [automatically closes](https://help.github.com/en/github/managing-your-work-on-github/closing-issues-using-keywords) the issue. <br>
Assign a reviewer and get in contact.

#### 4.0. Let someone else review your PR
Follow the GitHub guide [approving a pull request with required reviews](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/approving-a-pull-request-with-required-reviews). <br>
Assign one reviewer or a user group and get into contact.

If you are the reviewer:
- Check the changes in all corresponding files.
- Checkout the branch and run code.
- Comment if you would like to change something (Use `Request changes`)
- If all tests pass and all changes are good, `Approve` the PR. 
- Leave a comment and some nice words!

#### 4.1. Merge the PR
Follow the GitHub guide [merging a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/merging-a-pull-request).

#### 4.2. Delete the feature branch
Follow the GitHub guide [deleting a branch](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-and-deleting-branches-within-your-repository#deleting-a-branch).

### 5. Close the issue
Document the result in a few sentences and close the issue. <br>
Check that all steps have been documented:

- Issue title describes the problem you solved?
- All commit messages are linked in the issue?
- The branch was deleted?
- Entry in CHANGELOG.md?
- PR is closed?
- Issue is closed?
