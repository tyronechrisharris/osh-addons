package org.sensorhub.impl.datastore.postgis.database;

import org.junit.Test;
import org.sensorhub.api.module.IModuleProvider;

import java.util.ServiceLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Verifies that the tiered database is exposed through SensorHub module discovery. */
public class TestOscarTieredModuleProvider {
    @Test
    public void tieredDatabaseDescriptorIsServiceLoaderDiscoverable() {
        var providers = ServiceLoader.load(
                IModuleProvider.class,
                OscarTieredObsSystemDatabaseDescriptor.class.getClassLoader());

        var descriptor = providers.stream()
                .map(ServiceLoader.Provider::get)
                .filter(OscarTieredObsSystemDatabaseDescriptor.class::isInstance)
                .map(OscarTieredObsSystemDatabaseDescriptor.class::cast)
                .findFirst();

        assertTrue("OSCAR tiered database descriptor is missing from META-INF/services", descriptor.isPresent());
        assertEquals(OscarTieredObsSystemDatabase.class, descriptor.orElseThrow().getModuleClass());
        assertEquals(OscarTieredObsSystemDatabaseConfig.class, descriptor.orElseThrow().getModuleConfigClass());
    }
}
