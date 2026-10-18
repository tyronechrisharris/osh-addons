package org.sensorhub.impl.datastore.postgis.database;

import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.store.obs.OscarTieredObsStore;
import org.sensorhub.impl.datastore.postgis.store.obs.PostgisObsStoreImpl;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** PostGIS database module that exposes hot and cold observations as one OSH store. */
public class OscarTieredObsSystemDatabase extends PostgisObsSystemDatabase {
    private ScheduledExecutorService archiveExecutor;

    private TieredArchiveConfig archiveConfig() {
        if (!(config instanceof OscarTieredObsSystemDatabaseConfig tiered))
            throw new IllegalStateException("Tiered database requires OscarTieredObsSystemDatabaseConfig");
        return tiered.archive;
    }

    @Override
    protected PostgisObsStoreImpl createObsStore(String url, String dbName, String login, String password,
                                                  String tableName, int idScope,
                                                  IdProviderType idProviderType, boolean useBatch) {
        return new OscarTieredObsStore(url, dbName, login, password, tableName, idScope,
                idProviderType, archiveConfig());
    }

    @Override
    protected void afterStart() {
        super.afterStart();
        TieredArchiveConfig archive = archiveConfig();
        if (!archive.enabled)
            return;
        archiveExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "oscar-observation-archive");
            thread.setDaemon(true);
            return thread;
        });
        long delay = Math.max(1L, archive.intervalMinutes);
        // Run once immediately so a restarted or newly upgraded installation
        // begins bounded catch-up without waiting for the first interval.
        archiveExecutor.scheduleWithFixedDelay(this::runArchiveCycleSafely, 0L, delay, TimeUnit.MINUTES);
    }

    private void runArchiveCycleSafely() {
        try {
            runArchiveCycle();
        } catch (Throwable error) {
            getLogger().error("OSCAR observation archive cycle failed; hot data was retained", error);
        }
    }

    private void runArchiveCycle() {
        OscarTieredObsStore store = (OscarTieredObsStore) obsStore;
        TieredArchiveConfig archive = archiveConfig();
        Instant cutoff = Instant.now().minus(archive.hotRetentionDays, ChronoUnit.DAYS)
                .truncatedTo(ChronoUnit.HOURS);
        store.maintainArchiveLifecycle();
        Instant deadline = Instant.now().plus(archive.maximumCycleMinutes, ChronoUnit.MINUTES);
        int activated = 0;
        while (activated < archive.maximumGenerationsPerCycle && Instant.now().isBefore(deadline)) {
            // Work in immutable UTC hour windows. This bounds snapshots and
            // makes restart/idempotency behavior independent of local DST.
            Instant oldest = store.nextArchiveWindowStart(cutoff, "RAW_GATED").orElse(null);
            if (oldest == null)
                return;
            Instant start = utcHourWindowStart(oldest);
            Instant end = start.plus(1, ChronoUnit.HOURS);
            if (end.isAfter(cutoff))
                end = cutoff;
            end = store.selectArchiveWindowEnd(start, end);
            if (!start.isBefore(end))
                return;
            Instant selectedEnd = end;
            Optional<org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration> generation =
                    store.archiveWindow(start, end, "RAW_GATED");
            if (generation.isEmpty())
                return; // another owner has the window; avoid a tight retry loop
            activated++;
            getLogger().info("Activated archive generation {} with {} rows for [{}, {})",
                    generation.get().id(), generation.get().rowCount(), start, selectedEnd);
        }
        if (activated > 0)
            getLogger().info("OSCAR archive cycle completed {} generation(s); remaining backlog will continue next cycle",
                    activated);
    }

    /** Anchors every normal generation to a stable UTC Hive partition window. */
    public static Instant utcHourWindowStart(Instant oldestEligibleObservation) {
        if (oldestEligibleObservation == null)
            throw new IllegalArgumentException("Oldest eligible observation time is required");
        return oldestEligibleObservation.truncatedTo(ChronoUnit.HOURS);
    }

    @Override
    protected void beforeStop() {
        if (archiveExecutor != null) {
            archiveExecutor.shutdown();
            try {
                if (!archiveExecutor.awaitTermination(30, TimeUnit.SECONDS))
                    archiveExecutor.shutdownNow();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                archiveExecutor.shutdownNow();
            }
        }
        super.beforeStop();
    }
}
