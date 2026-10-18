package org.sensorhub.impl.datastore.postgis.archive;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestArchiveLifecycleInvariants {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void onlyFailedOrStaleExportingJobsAreRetried() {
        assertTrue(ArchiveCatalog.shouldRetryJob("FAILED", false));
        assertTrue(ArchiveCatalog.shouldRetryJob("EXPORTING", true));
        assertFalse(ArchiveCatalog.shouldRetryJob("EXPORTING", false));
        assertFalse(ArchiveCatalog.shouldRetryJob("ACTIVE", true));
        assertFalse(ArchiveCatalog.shouldRetryJob("SEALED", true));
    }

    @Test
    public void archiveJobRevisionsIncreaseMonotonically() {
        assertEquals(1, ArchiveCatalog.nextRevision(0));
        assertEquals(42, ArchiveCatalog.nextRevision(41));
        assertThrows(ArithmeticException.class, () -> ArchiveCatalog.nextRevision(Integer.MAX_VALUE));
    }

    @Test
    public void exactObservationHoldsAreExcludedFromDeletionBatch() {
        assertEquals(List.of(10L, 12L), ArchiveLifecycleManager.withoutHeld(
                List.of(10L, 11L, 12L, 13L), Set.of(11L, 13L)));
    }

    @Test
    public void lifecycleRejectsSymbolicLinkArchiveObjects() throws Exception {
        Path root = temporary.newFolder("archive-root").toPath();
        Path outside = temporary.newFile("outside.parquet").toPath();
        Path link = root.resolve("linked.parquet");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }

        assertThrows(IllegalStateException.class,
                () -> ArchiveLifecycleManager.resolveArchiveObject(root, Path.of("linked.parquet")));
    }
}
