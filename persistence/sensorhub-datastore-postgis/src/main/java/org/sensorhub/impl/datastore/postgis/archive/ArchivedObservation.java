package org.sensorhub.impl.datastore.postgis.archive;

import java.time.Instant;

/** Storage-neutral observation row retained in Parquet and mutation overlays. */
public record ArchivedObservation(
        int idScope,
        long id,
        int dataStreamScope,
        long dataStreamId,
        Integer foiScope,
        Long foiId,
        Instant phenomenonTime,
        Instant resultTime,
        String resultJson,
        String profile) {
}
