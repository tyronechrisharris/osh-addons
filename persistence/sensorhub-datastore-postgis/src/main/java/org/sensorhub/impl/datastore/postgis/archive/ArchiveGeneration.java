package org.sensorhub.impl.datastore.postgis.archive;

import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

/** Immutable catalog projection used by the query planner and lifecycle tools. */
public record ArchiveGeneration(
        UUID id,
        String profile,
        Instant start,
        Instant end,
        Path objectPath,
        String sha256,
        long rowCount,
        long minimumId,
        long maximumId,
        String state,
        boolean parityVerified) {
}
