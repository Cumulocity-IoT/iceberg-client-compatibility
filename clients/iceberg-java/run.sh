#!/usr/bin/env bash
# Usage, from the repository root: clients/iceberg-java/run.sh <plan.json> <out.json>
set -euo pipefail
. harness/env.sh

dir="$PWD/clients/iceberg-java"
mvn -q -f "$dir/pom.xml" compile dependency:build-classpath -Dmdep.outputFile="$dir/target/classpath.txt"
java --enable-native-access=ALL-UNNAMED -cp "$dir/target/classes:$(cat "$dir/target/classpath.txt")" Checks "$1" "$2"
