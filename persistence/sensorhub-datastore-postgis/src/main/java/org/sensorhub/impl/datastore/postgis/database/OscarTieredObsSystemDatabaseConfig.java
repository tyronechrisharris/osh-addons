package org.sensorhub.impl.datastore.postgis.database;

import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;

/** Production OSCAR database configuration with an OSH-compatible Parquet cold tier. */
public class OscarTieredObsSystemDatabaseConfig extends PostgisObsSystemDatabaseConfig {
    @DisplayInfo(label = "Cold archive")
    public TieredArchiveConfig archive = new TieredArchiveConfig();

    public OscarTieredObsSystemDatabaseConfig() {
        this.moduleClass = OscarTieredObsSystemDatabase.class.getCanonicalName();
        // The tiered store snapshots mutable DataBlocks and commits real JDBC
        // batches; this is required to avoid one PostgreSQL transaction and
        // one datastream-range update per telemetry sample.
        this.useBatch = true;
    }
}
