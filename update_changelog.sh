#!/bin/bash
set -euo pipefail

which docker > /dev/null || (echoerr "Please ensure that docker is installed" && exit 1)

cd -P -- "$(dirname -- "$0")" # switch to this dir

CHANGELOG_FILE=CHANGELOG.md
previous_version=$(head -5 $CHANGELOG_FILE | grep "##" | awk -F']' '{print $1}' | cut -c 5-)

if [[ -z ${GITHUB_TOKEN-} ]]; then
  echoerr "**WARNING** GITHUB_TOKEN is not currently set"
  exit 1
fi


# Remove the header so we can append the additions
tail -n +2 "$CHANGELOG_FILE" > "$CHANGELOG_FILE.tmp" && mv "$CHANGELOG_FILE.tmp" "$CHANGELOG_FILE"

docker run -it --rm -v "$(pwd)":/usr/local/src/your-app ferrarimarco/github-changelog-generator -u stargate -p stargate -t $GITHUB_TOKEN --since-tag $previous_version -b $CHANGELOG_FILE


# Remove the additional footer added
tail -r "$CHANGELOG_FILE" | tail -n +4 | tail -r > "$CHANGELOG_FILE.tmp" && mv "$CHANGELOG_FILE.tmp" "$CHANGELOG_FILE"
