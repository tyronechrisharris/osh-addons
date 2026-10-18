package org.sensorhub.impl.datastore.postgis.archive;

import org.sensorhub.api.config.DisplayInfo;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
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

    @Min(16)
    @DisplayInfo(label = "DuckDB memory limit (MiB)",
            desc = "Per archive export, verification, or cold-query connection; larger work spills to the bounded temporary directory")
    public int duckDbMemoryLimitMegabytes = 256;

    @Min(1)
    @Max(64)
    @DisplayInfo(label = "DuckDB threads",
            desc = "Maximum worker threads used by each archive export, verification, or cold-query connection")
    public int duckDbThreads = 2;

    @Min(16)
    @DisplayInfo(label = "DuckDB temporary limit (MiB)",
            desc = "Maximum spill space used by one archive operation")
    public long duckDbMaximumTemporaryMegabytes = 4096;

    @Min(1)
    @Max(32)
    @DisplayInfo(label = "Maximum concurrent DuckDB sessions",
            desc = "Process-wide admission limit shared by archive exports, verification, deletion, and cold queries")
    public int duckDbMaximumConcurrentSessions = 2;

    @Min(1)
    @Max(300)
    @DisplayInfo(label = "DuckDB admission timeout (seconds)",
            desc = "Maximum time an archive operation waits for process-wide DuckDB capacity before failing busy")
    public int duckDbSessionAcquireTimeoutSeconds = 30;

    @DisplayInfo(label = "DuckDB temporary directory",
            desc = "Private writable parent for per-operation spill directories; do not place it inside the immutable archive tree")
    public String duckDbTemporaryDirectory = "/var/lib/oscar/duckdb-tmp";

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
        if (duckDbMemoryLimitMegabytes < 16)
            throw new IllegalArgumentException("duckDbMemoryLimitMegabytes must be at least 16 MiB");
        if (duckDbThreads < 1 || duckDbThreads > 64)
            throw new IllegalArgumentException("duckDbThreads must be between 1 and 64");
        if (duckDbMaximumTemporaryMegabytes < 16)
            throw new IllegalArgumentException("duckDbMaximumTemporaryMegabytes must be at least 16 MiB");
        if (duckDbMaximumConcurrentSessions < 1 || duckDbMaximumConcurrentSessions > 32)
            throw new IllegalArgumentException(
                    "duckDbMaximumConcurrentSessions must be between 1 and 32");
        if (duckDbSessionAcquireTimeoutSeconds < 1 || duckDbSessionAcquireTimeoutSeconds > 300)
            throw new IllegalArgumentException(
                    "duckDbSessionAcquireTimeoutSeconds must be between 1 and 300");
        if (duckDbTemporaryDirectory == null || duckDbTemporaryDirectory.isBlank())
            throw new IllegalArgumentException("duckDbTemporaryDirectory is required");
        if (rootDirectory == null || rootDirectory.isBlank())
            throw new IllegalArgumentException("rootDirectory is required");
        final Path archiveRoot;
        final Path temporaryRoot;
        final Path persistedStateRoot;
        try {
            archiveRoot = Path.of(rootDirectory).toAbsolutePath().normalize();
            temporaryRoot = Path.of(duckDbTemporaryDirectory).toAbsolutePath().normalize();
            persistedStateRoot = Path.of("/var/lib/oscar").toAbsolutePath().normalize();
        } catch (InvalidPathException error) {
            throw new IllegalArgumentException("Archive and DuckDB temporary paths must be valid", error);
        }
        Path reservedStateTemporaryRoot = persistedStateRoot.resolve("duckdb-tmp");
        if (temporaryRoot.startsWith(persistedStateRoot) &&
                !temporaryRoot.equals(reservedStateTemporaryRoot))
            throw new IllegalArgumentException(
                    "DuckDB scratch inside /var/lib/oscar must use /var/lib/oscar/duckdb-tmp so backups exclude it");
        if (archiveRoot.equals(temporaryRoot) || archiveRoot.startsWith(temporaryRoot) ||
                temporaryRoot.startsWith(archiveRoot))
            throw new IllegalArgumentException(
                    "DuckDB temporary directory and immutable archive root must be disjoint");
    }
}
