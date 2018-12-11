#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
FLO_VERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
mvn -B install -DskipTests
cd flo-tests

pushd shading
mvn -B clean install -DskipTests -Dflo.version=$FLO_VERSION
popd

pushd shading-user
mvn -B clean test
popd
