package org.sensorhub.impl.datastore.postgis.archive;

import java.sql.Connection;

@FunctionalInterface
public interface SqlConnectionProvider {
    Connection open();
}
