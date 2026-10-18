package org.sensorhub.impl.datastore.postgis.archive;

import org.sensorhub.api.config.DisplayInfo;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Configuration for OSCAR's immutable Parquet observation tier.
 *
 * <p>The defaults deliberately copy data without deleting PostgreSQL rows.
 * Hot deletion is a separately controlled rollout step and requires verified
 * query parity plus a rollback grace period.</p>
 */
public class TieredArchiveConfig {
    @DisplayInfo(label = "Enable cold archive")
    public boolean enabled = true;

    @DisplayInfo(label = "Archive root", desc = "Durable path mounted outside the PostgreSQL volume")
    public String rootDirectory = "/var/lib/oscar/archive";

    @Min(1)
    @DisplayInfo(label = "Hot retention (days)")
    public int hotRetentionDays = 30;

    @Min(1)
    @DisplayInfo(label = "Archive interval (minutes)")
    public int intervalMinutes = 60;

    @Min(1)
    @DisplayInfo(label = "Maximum generations per cycle",
            desc = "Bounds catch-up work while allowing an installation with a backlog to archive more than one window per interval")
    public int maximumGenerationsPerCycle = 24;

    @Min(1)
    @DisplayInfo(label = "Maximum archive cycle duration (minutes)",
            desc = "Stops starting new generations after this duration; an in-progress generation is allowed to finish safely")
    public int maximumCycleMinutes = 15;

    @Min(1000)
    @DisplayInfo(label = "Maximum rows per generation")
    public int maximumRowsPerGeneration = 2_000_000;

    @Min(1)
    @DisplayInfo(label = "Maximum uncompressed generation bytes",
            desc = "Bounds JSON and row payload admitted to one archive generation before Parquet compression")
    public long maximumUncompressedBytesPerGeneration = 2L * 1024 * 1024 * 1024;

    @Min(1)
    @DisplayInfo(label = "Delete transaction size")
    public int deleteBatchSize = 5_000;

    @Min(1)
    @DisplayInfo(label = "Rollback grace (days)")
    public int rollbackGraceDays = 7;

    @DisplayInfo(label = "Delete verified hot rows", desc = "Keep false until mixed-tier query parity and restore tests pass")
    public boolean deleteHotAfterArchive = false;

    @DisplayInfo(label = "Require parity receipt (mandatory)",
            desc = "Safety invariant: false is rejected; hot deletion always requires a successful parity receipt")
    public boolean requireParityReceipt = true;

    @Min(1)
    @Max(22)
    @DisplayInfo(label = "ZSTD compression level")
    public int zstdCompressionLevel = 6;

    @DisplayInfo(label = "Archive output names", desc = "Only high-volume raw/rollup streams are cold-tiered by default")
    public List<String> outputNames = new ArrayList<>(Arrays.asList(
            "gammaCounts", "gammaThreshold", "neutronCounts", "setupGamma", "setupNeutron"));

    /** Rejects unsafe compatibility settings that cannot weaken invariants. */
    public void validate() {
        if (!requireParityReceipt)
            throw new IllegalArgumentException(
                    "requireParityReceipt is a mandatory safety invariant and cannot be disabled");
        if (maximumRowsPerGeneration < 1)
            throw new IllegalArgumentException("maximumRowsPerGeneration must be positive");
        if (maximumUncompressedBytesPerGeneration < 1)
            throw new IllegalArgumentException("maximumUncompressedBytesPerGeneration must be positive");
    }
}
