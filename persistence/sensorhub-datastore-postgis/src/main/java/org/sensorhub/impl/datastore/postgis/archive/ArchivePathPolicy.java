package org.sensorhub.impl.datastore.postgis.archive;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/** Shared no-symlink confinement policy for archive reads and publication. */
final class ArchivePathPolicy {
    private ArchivePathPolicy() {
    }

    static Path prepareDirectory(Path configuredRoot, Path requestedDirectory) throws Exception {
        Path root = configuredRoot.toAbsolutePath().normalize();
        Files.createDirectories(root);
        Path realRoot = root.toRealPath();
        Path directory = requestedDirectory.toAbsolutePath().normalize();
        if (!directory.startsWith(root))
            throw new IllegalStateException("Archive publication path escapes configured root");

        Path current = root;
        for (Path component : root.relativize(directory)) {
            current = current.resolve(component);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                BasicFileAttributes attributes = Files.readAttributes(
                        current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (attributes.isSymbolicLink() || !attributes.isDirectory())
                    throw new IllegalStateException(
                            "Archive publication path contains a non-directory or symbolic link: " + current);
            } else {
                Files.createDirectory(current);
                BasicFileAttributes attributes = Files.readAttributes(
                        current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (!attributes.isDirectory() || attributes.isSymbolicLink())
                    throw new IllegalStateException("Archive directory creation was redirected: " + current);
            }
        }
        Path realDirectory = directory.toRealPath();
        if (!realDirectory.startsWith(realRoot))
            throw new IllegalStateException("Archive publication directory resolves outside configured root");
        return directory;
    }

    static Path prepareWriteTarget(Path configuredRoot, Path target) throws Exception {
        Path normalized = target.toAbsolutePath().normalize();
        prepareDirectory(configuredRoot, normalized.getParent());
        if (Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
            BasicFileAttributes attributes = Files.readAttributes(
                    normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (attributes.isSymbolicLink() || !attributes.isRegularFile())
                throw new IllegalStateException("Archive write target is not a regular file: " + normalized);
        }
        return normalized;
    }
}
