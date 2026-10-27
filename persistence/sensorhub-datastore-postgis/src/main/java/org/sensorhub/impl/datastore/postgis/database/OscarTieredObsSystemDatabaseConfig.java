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
        this.archive.enabled = true;
        this.archive.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        this.archive.hotRetentionDays = 2;
        this.archive.parquetPrimaryArchiveDelayMinutes = 5;
        this.archive.immutableStageWindowMinutes = 30;
        this.archive.maximumFuturePhenomenonSkewMinutes = 5;
        this.archive.parquetPrimaryIntervalMinutes = 1;
        this.archive.maximumStagePartitionsPerCycle = 8;
        this.archive.intervalMinutes = 60;
        this.archive.maximumGenerationsPerCycle = 2;
        this.archive.maximumVerificationsPerCycle = 2;
        this.archive.maximumDeletionsPerCycle = 2;
        this.archive.maximumCycleMinutes = 15;
        this.archive.deleteBatchSize = 5_000;
        this.archive.rollbackGraceDays = 3;
        this.archive.deleteHotAfterArchive = true;
        this.archive.requireParityReceipt = true;
        this.archive.zstdCompressionLevel = 6;
    }
}
