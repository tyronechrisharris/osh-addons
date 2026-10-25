#!/usr/bin/env bash
set -Eeuo pipefail

git diff --check
./gradlew --no-daemon --no-parallel :sensorhub-datastore-postgis:test \
  --tests org.sensorhub.impl.datastore.postgis.TestTieredArchiveConfiguration \
  --tests org.sensorhub.impl.datastore.postgis.TestParameterizedQueryBuilder \
  --tests org.sensorhub.impl.datastore.postgis.TestParquetArchiveReader
