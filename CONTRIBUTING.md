# CONTRIBUTE

## Introduction
We encourage everyone to help us improve Taegis Magic by contributing to the project. Contributions can be as small as documentation updates or adding example use cases, to adding commenting or properly styling code segments, to full feature contributions. We ask that contributors follow our established guidelines for contributing to the project.

These guidelines are based on the [omnia project](https://github.com/dellhpc/omnia).

This document will evolve as the project matures. Please be sure to regularly refer back in order to stay in-line with contribution guidelines.

## How to Contribute to Taegis Magic
Contributions to Taegis Magic are made through [Pull Requests (PRs)](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests). To make a pull request against Taegis Magic, use the following steps:

1. **Create an issue:** [Create an issue](https://help.github.com/en/github/managing-your-work-on-github/creating-an-issue) and describe what you are trying to solve. It does not matter whether it is a new feature, a bug fix, or an improvement. All pull requests need to be associated to an issue. When creating an issue, be sure to use the appropriate issue template (bug fix or feature request) and complete all of the required fields. If your issue does not fit in either a bug fix or feature request, then create a blank issue and be sure to including the following information:
   * **Problem description:** Describe what you believe needs to be addressed
   * **Problem location:** In which file and at what line does this issue occur?
   * **Suggested resolution:** How do you intend to resolve the problem?
2. **Create a personal fork:** All work on Taegis Magic should be done in a [fork of the repository](https://help.github.com/en/github/getting-started-with-github/fork-a-repo). Only the maintiners are allowed to commit directly to the project repository.
3. **Issue branch:** [Create a new branch](https://help.github.com/en/desktop/contributing-to-projects/creating-a-branch-for-your-work) on your fork of the repository. All contributions should be branched from `main`. Use `git checkout main; git checkout -b <new-branch-name>` to create the new branch.
   * **Branch name:** The branch name should be based on the issue you are addressing. Use the following pattern to create your new branch name: issue-number, e.g., issue-1023.
4. **Commit changes to the issue branch:** It is important to commit your changes to the issue branch. Commit messages should be descriptive of the changes being made.
   * **Signing your commits:** All commits to Taegis Magic need to be signed with the [Developer Certificate of Origin (DCO)](https://developercertificate.org/) in order to certify that the contributor has permission to contribute the code. In order to sign commits, use either the `--signoff` or `-s` option to `git commit`:
   ```
   git commit --signoff
   git commit -s
   ```
   Make sure you have your user name and e-mail set. The `--signoff | -s` option will use the configured user name and e-mail, so it is important to configure it before the first time you commit. Check the following references:

      * [Setting up your github user name](https://help.github.com/articles/setting-your-username-in-git/)
      * [Setting up your e-mail address](https://help.github.com/articles/setting-your-commit-email-address-in-git/)
   
5. **Push the changes to your personal repo:** To be able to create a pull request, push the changes to origin: `git push origin <new-branch-name>`. Here I assume that `origin` is your personal repo, e.g., `mpegman/taegis-magic.git`.
6. **Create a pull request:** [Create a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request) with a title following this format Issue ###: Description (_i.e., Issue 1023: Reformat testutils_). It is important that you do a good job with the description to make the job of the code reviewer easier. A good description not only reduces review time, but also reduces the probability of a misunderstanding with the pull request.
   * **Important:** When preparing a pull request it is important to stay up-to-date with the project repository. We recommend that you rebase against the upstream repo _frequently_. To do this, use the following commands:
   ```
   git pull --rebase upstream devel #upstream is secureworks/taegis-magic
   git push --force origin <pr-branch-name> #origin is your fork of the repository (e.g., <github_user_name>/taegis-magic.git)
   ```
   * **PR Description:** Be sure to fully describe the pull request. Ideally, your PR description will contain:
      1. A description of the main point (_e.g., why was this PR made?_),
      2. Linking text to the related issue (_e.g., This PR closes issue #<issue_number>_),
      3. How the changes solves the problem, and
      4. How to verify that the changes work correctly.
   
## Taegis Magic Branches and Contribution Flow
Taegis Magic has one lifetime branch: `main`.  The `main` branch is also the default branch for the project.

## Developer Certificate of Origin
Contributions to Taegis Magic must be signed with the [Developer Certificate of Origin (DCO)](https://developercertificate.org/):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```