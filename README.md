# koji-tools

Supplementary tools and utilities for the Koji build system

* `koji-buildroots-with-build` - list all the buildroots that contain a
  particular build.

* `koji-builds-in-common` - Compare builds between multiple tags. Report
  builds and packages in common.

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

* `koji-remove-arch` - Globally remove an architecture from all tags in
  Koji.

* `koji-remove-stray-build` - Remove stray build content. Useful for
  cleaning up build artifacts that have no records in Koji, or are
  Failed, Deleted, or Canceled.

* `koji-rpmdiff` - Show differences between packages

* `koji-show-tag-space` - Show the disk space statistics for builds in a
  tag

* `koji-tag-overlap` - Show package overlaps for a set of tags

* `kojitop` - Show the tasks each builder is working on

* `koji-dump-hosts` - Write current host data to a file

* `koji-restore-hosts` - Restore host data from a file
