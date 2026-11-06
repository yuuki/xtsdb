#!/bin/sh
# Prints the CHANGELOG.md section of a version, without its heading.
# Fails when the version has no section, so that a release cannot be tagged
# before its changes are described.
#
#   scripts/release-notes.sh 2.0.0
set -eu

version=${1:?usage: $0 <version>}
changelog=${2:-CHANGELOG.md}

awk -v version="$version" '
	/^## \[/ || /^\[[^]]+\]: / {
		if (found) exit
		if (index($0, "## [" version "]") == 1) { found = 1; next }
	}
	found { print }
	END { if (!found) exit 1 }
' "$changelog" > /tmp/release-notes.$$ || {
	echo "error: $changelog has no section for $version" >&2
	rm -f /tmp/release-notes.$$
	exit 1
}
# Trim leading and trailing blank lines.
sed -e '/./,$!d' /tmp/release-notes.$$ | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}'
rm -f /tmp/release-notes.$$
