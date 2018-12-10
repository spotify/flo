#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
FLO_VERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
mvn -B install -DskipTests
cd flo-tests

pushd shading
mvn versions:set -DnewVersion=$FLO_VERSION
mvn -B clean install -DskipTests
popd

pushd shading-user
mvn versions:set -DnewVersion=$FLO_VERSION
mvn -B clean test
popd
