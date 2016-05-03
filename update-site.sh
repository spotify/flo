#!/usr/bin/env bash

git checkout v$1
mvn clean site site:deploy
g checkout gh-pages
mv website/maven maven/$1
rm -rf maven/latest
cp -r maven/$1 maven/latest
git add .
git commit -m 'update maven site'
git push
git checkout master
