#!/usr/bin/env bash

set +ex

V=$(git describe --tags $(git rev-list --tags --max-count=1))
V=${V#v}

git checkout v$V
mvn clean site site:deploy
git checkout gh-pages
mv website/maven maven/$V
rm -rf maven/latest
cp -r maven/$V maven/latest
git add --all
git commit -m "update maven site for v$V"
git push
git checkout master
