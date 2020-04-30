# koji-tools

Supplementary tools and utilities for the Koji build system

* `koji-buildroots-with-build` - list all the buildroots that contain a
  particular build.

* `koji-builds-in-common` - Compare builds between multiple tags. Report
  builds and packages in common.

* `koji-cancel-broken-builds` - Cancel "running" builds without any tasks.

* `koji-change-volumes` - Move builds to volumes according to a policy

* `koji-check-builds` - Run various sanity-checks on builds stored
  within Koji

* `koji-compare-buildroots` - Compare the contents of the buildroots of
  two builds, and print out their differences in a readable format.

* `koji-compare-tags` - Compare builds between 2 or more tags. Print out
  builds and packages in common.

* `koji-find-hidden-packages` - Find and print the "hidden" packages of
  the given tag

* `koji-get-rpm-fields-in-build` - Specify a build and an RPM field and
  this script will print out that field from the SRPM for that build.
  Useful for comparing changelogs or reviewing licenses.

* `koji-get-rpm-fields-in-tag` - Specify a Koji tag and an RPM field and
  this script will print out that field from the SRPM for each build in
  that tag.  Useful for comparing changelogs or reviewing licenses.

* `koji-inheritance-replace` - Replace one parent tag with another
  parent tag.

* `koji-list-built-with` - List all the builds that had a given build in
  the buildroot.

* `koji-prune-scratch` - Sample cronjob for cleaning scratch build data

* `koji-prune-work` - Sample cronjob for cleaning work directories

* `koji-remove-arch` - Globally remove an architecture from all tags in
  Koji.

* `koji-remove-stray-build` - Remove stray build content. Useful for
  cleaning up build artifacts that have no records in Koji, or are
  Failed, Deleted, or Canceled.

* `koji-rpmdiff` - Show differences between packages

* `koji-search-containers` - Search container image builds for an RPM build.

* `koji-show-tag-space` - Show the disk space statistics for builds in a
  tag

* `koji-ssl-admin` - Generates various SSL certificates

* `koji-tag-overlap` - Show package overlaps for a set of tags

* `kojitop` - Show the tasks each builder is working on

* `koji-dump-hosts` - Write current host data to a file

* `koji-restore-hosts` - Restore host data from a file

* `koji-multitag` - Perform multiple tag actions on multiple builds

# Development

`koji-tools` is low-barrier repository of various scripts related to usage and
administration scripts related to [koji](https://pagure.io/koji/). Feel free to
submit any script, which can be useful for others. Mature scripts can be also
promoted to koji itself either as plugins or as part of the base koji.

Pull requests submitted to this repository will generally go through review of
one of the koji core developers and will be merged, if there is no significant
issue with them. They should follow Koji's programming
[guidelines](https://docs.pagure.org/koji/writing_koji_code/), but it is not
strictly required.

Scripts which should move to base koji are under harder scrutiny. They should be
ready to land there, contain basic tests, etc. Filing an [issue in
koji](https://pagure.io/koji/new_issue) is good place to start. We can help you
to get that code in the shape, when it will be in pull request.

Thanks for contributions!

# Releases

There are no official releases made from this repo. Users usually pull it via
git and use directly. We're not planning to make official releases of this repo
in near future.

