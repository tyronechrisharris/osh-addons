package org.sensorhub.impl.datastore.postgis.database;

import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.impl.module.JarModuleProvider;

/** Makes the OSCAR tiered database selectable in SensorHub Admin. */
public class OscarTieredObsSystemDatabaseDescriptor extends JarModuleProvider {
    @Override
    public String getModuleName() {
        return "OSCAR Tiered Observation Database";
    }

    @Override
    public String getModuleDescription() {
        return "PostGIS hot storage with immutable, checksummed Parquet cold observations";
    }

    @Override
    public Class<? extends IModule<?>> getModuleClass() {
        return OscarTieredObsSystemDatabase.class;
    }

    @Override
    public Class<? extends ModuleConfig> getModuleConfigClass() {
        return OscarTieredObsSystemDatabaseConfig.class;
    }
}
