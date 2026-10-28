/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2023 Georobotix. All Rights Reserved.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.store.obs;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.postgresql.util.PGobject;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.obs.DataStreamInfoWrapper;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.store.PostgisStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderDataStreamStore;
import org.sensorhub.impl.datastore.postgis.utils.PostgisUtils;
import org.sensorhub.impl.datastore.postgis.utils.SerializerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgisDataStreamStoreImpl extends PostgisStore<QueryBuilderDataStreamStore> implements IDataStreamStore {
    private volatile Cache<Long, IDataStreamInfo> cache = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();

    private static final Logger logger = LoggerFactory.getLogger(PostgisDataStreamStoreImpl.class);
    private final Lock lock = new ReentrantLock();
    protected PostgisObsStoreImpl obsStore;
    protected ISystemDescStore systemStore;

    public PostgisDataStreamStoreImpl(PostgisObsStoreImpl obsStore, String url, String dbName, String login, String password,
                                      int idScope, IdProviderType dsIdProviderType, boolean useBatch) {
        super(idScope, dsIdProviderType, new QueryBuilderDataStreamStore(obsStore.getDatastoreName() + "_datastreams"), false);
        this.init(obsStore, url,dbName,login,password);
    }

    protected void init(PostgisObsStoreImpl obsStore, String url, String dbName, String login, String password) {
        this.obsStore = Asserts.checkNotNull(obsStore, PostgisObsStoreImpl.class);
        queryBuilder.linkTo(obsStore);
        super.init(url, dbName, login, password, new String[]{
                queryBuilder.createTableQuery(),
                queryBuilder.createIndexQuery(),
                queryBuilder.createUniqueIndexQuery(),
                queryBuilder.createValidTimeBeginIndexQuery(),
                queryBuilder.createValidTimeEndIndexQuery(),
                queryBuilder.createTrigramExtensionQuery(),
                queryBuilder.createTrigramDescriptionFullTextIndexQuery(),
                queryBuilder.createSchemaFieldsIndexQuery(),
                queryBuilder.createMinMaxTimeTriggerQuery(),
                queryBuilder.createIdIndex()
        });

        // The base initializer only executes schema statements when the table
        // is first created.  Re-apply this idempotent trigger definition so an
        // upgrade replaces the legacy FOR EACH ROW trigger as well.
        try (Connection connection = this.connectionManager.getConnection()) {
            PostgisUtils.executeQueries(connection, new String[]{
                    queryBuilder.createMinMaxTimeTriggerQuery(),
                    queryBuilder.migrateLegacyDailyFileDataStreamSchemaQuery()
            });
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot install batched datastream range maintenance", e);
        }
    }

    /** Runtime pool bound used by constrained deployments and deadlock probes. */
    public void setMaximumPoolSize(int maximumPoolSize) {
        connectionManager.setMaximumPoolSize(maximumPoolSize);
    }

    protected class DataStreamInfoWithTimeRanges extends DataStreamInfoWrapper
    {
        Long dsID;
        final IDataStreamInfo baseInfo;
        TimeExtent validTime;
        TimeExtent phenomenonTimeRange;
        TimeExtent resultTimeRange;

        DataStreamInfoWithTimeRanges(Long internalID, IDataStreamInfo dsInfo)
        {
            super(dsInfo instanceof DataStreamInfoWithTimeRanges existing ? existing.baseInfo : dsInfo);
            this.dsID = internalID;
            this.baseInfo = dsInfo instanceof DataStreamInfoWithTimeRanges existing ? existing.baseInfo : dsInfo;
        }

        @Override
        public TimeExtent getValidTime()
        {
            if (validTime == null)
            {
                validTime = super.getValidTime();

                // if valid time ends at now and there is a more recent version, compute the actual end time
                /*if (validTime.endsNow())
                {
                    var sysDsKey = new MVTimeSeriesSystemKey(
                            getSystemID().getInternalID().getIdAsLong(),
                            getOutputName(),
                            getValidTime().begin());

                    var nextKey = dataStreamBySystemIndex.lowerKey(sysDsKey); // use lower cause time sorting is reversed
                    if (nextKey != null &&
                            nextKey.systemID == sysDsKey.systemID &&
                            nextKey.signalName.equals(sysDsKey.signalName))
                        validTime = TimeExtent.period(validTime.begin(), Instant.ofEpochSecond(nextKey.validStartTime));
                }*/
            }

            return validTime;
        }

        @Override
        public TimeExtent getPhenomenonTimeRange()
        {
//            if (phenomenonTimeRange == null)
//                phenomenonTimeRange = obsStore.getDataStreamPhenomenonTimeRange(dsID);

            return phenomenonTimeRange;
        }

        @Override
        public TimeExtent getResultTimeRange()
        {
//            if (resultTimeRange == null)
//                resultTimeRange = obsStore.getDataStreamResultTimeRange(dsID);

            return resultTimeRange;
        }

        public void setPhenomenonTimeRange(TimeExtent phenomenonTimeRange) {
            this.phenomenonTimeRange = phenomenonTimeRange;
        }

        public void setResultTimeRange(TimeExtent resultTimeRange) {
            this.resultTimeRange = resultTimeRange;
        }

        public void setValidTime(TimeExtent validTime) {
            this.validTime = validTime;
        }
    }

    @Override
    public Stream<DataStreamKey> selectKeys(DataStreamFilter filter) {
        ParameterizedQuery query = queryBuilder.createParameterizedSelectKeysQuery(filter);
        List<DataStreamKey> results = new ArrayList<>();
        try (Connection connection = this.connectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(query.sql())) {
            query.bind(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    results.add(new DataStreamKey(idScope, resultSet.getLong("id")));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return results.stream();
    }

    @Override
    public Stream<Entry<DataStreamKey, IDataStreamInfo>> selectEntries(DataStreamFilter filter, Set<DataStreamInfoField> fields) {
        // build request
        ParameterizedQuery query = queryBuilder.createParameterizedSelectEntriesQuery(filter, fields);

        Map<DataStreamKey, IDataStreamInfo> results = new LinkedHashMap<>();

        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
                query.bind(statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        long id = resultSet.getLong("id");
                        IDataStreamInfo cached = cache.getIfPresent(id);
                        IDataStreamInfo dataStreamInfo;
                        if (cached instanceof DataStreamInfoWithTimeRanges cachedWrapper) {
                            dataStreamInfo = cachedWrapper.baseInfo;
                        } else if (cached != null) {
                            dataStreamInfo = cached;
                        } else {
                            String data = resultSet.getString("data");
                            dataStreamInfo = SerializerUtils.readIDataStreamInfoFromJson(data);
                        }
                        DataStreamInfoWithTimeRanges wrapper = new DataStreamInfoWithTimeRanges(id, dataStreamInfo);

                        Instant minPhenomenonTime = UtcJdbcTime.getInstant(resultSet, "min_phenomenon_time");
                        Instant maxPhenomenonTime = UtcJdbcTime.getInstant(resultSet, "max_phenomenon_time");
                        Instant minResultTime = UtcJdbcTime.getInstant(resultSet, "min_result_time");
                        Instant maxResultTime = UtcJdbcTime.getInstant(resultSet, "max_result_time");

                        wrapper.setPhenomenonTimeRange(TimeExtent.period(
                                minPhenomenonTime != null ? minPhenomenonTime : Instant.MIN,
                                maxPhenomenonTime != null ? maxPhenomenonTime : Instant.MAX
                        ));

                        wrapper.setResultTimeRange(TimeExtent.period(
                                minResultTime != null ? minResultTime : Instant.MIN,
                                maxResultTime != null ? maxResultTime : Instant.MAX
                        ));

                        cache.put(id, wrapper);
                        results.put(new DataStreamKey(idScope, id), wrapper);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        logger.debug("{}, {}", query.sql(), results.size());
        return results.entrySet().stream();
    }

    @Override
    public void commit() {
        try {
            obsStore.commit();
        } catch (DataStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Entry<DataStreamKey, IDataStreamInfo> getLatestVersionEntry(String sysUID, String outputName) {
        return selectEntries(new DataStreamFilter.Builder()
                .withSystems()
                    .withUniqueIDs(sysUID)
                    .done()
                .withOutputNames(outputName)
                .withValidTime(new TemporalFilter.Builder().withLatestTime().build())
                .build())
                .findFirst()
                .orElse(null);
    }

    public void intersectsAndUpdate(IDataStreamInfo dsInfo) throws DataStoreException {
        // if DS already exists, compute the time intersection
        DataStreamFilter.Builder dataStreamFilterBuilder = new DataStreamFilter.Builder();
        if (dsInfo.getSystemID() != null) {
            dataStreamFilterBuilder = dataStreamFilterBuilder.withSystems(dsInfo.getSystemID().getInternalID());
        }
        if (dsInfo.getOutputName() != null) {
            dataStreamFilterBuilder = dataStreamFilterBuilder.withOutputNames(dsInfo.getOutputName());
        }

        if (dsInfo.getValidTime() != null) {
            dataStreamFilterBuilder = dataStreamFilterBuilder.withValidTime(new TemporalFilter.Builder()
                    .withLatestTime()
                    .build());
        } else {
            return;
        }

        DataStreamFilter dataStreamFilter = dataStreamFilterBuilder.build();
        Stream<Entry<DataStreamKey, IDataStreamInfo>> existingDsInfo = this.selectEntries(dataStreamFilter);
        Iterator<Entry<DataStreamKey, IDataStreamInfo>> ite = existingDsInfo.iterator();

        TimeExtent newValidTime = dsInfo.getValidTime();
        while (ite.hasNext()) {
            Entry<DataStreamKey, IDataStreamInfo> entry = ite.next();
            IDataStreamInfo currentDataStreamInfo = entry.getValue();

            TimeExtent dbValidTime = currentDataStreamInfo.getValidTime();
            if (dbValidTime == null) {
                Instant begin = Instant.EPOCH;
                TimeExtent phenRange = currentDataStreamInfo.getPhenomenonTimeRange();
                if (phenRange != null && phenRange.begin() != null
                        && !phenRange.begin().equals(Instant.MIN)
                        && !phenRange.begin().isAfter(newValidTime.begin())) {
                    begin = phenRange.begin();
                } else if (newValidTime.begin().isBefore(begin)) {
                    begin = newValidTime.begin();
                }
                dbValidTime = TimeExtent.endNow(begin);
            }

            TimeExtent resultIntersection;
            if (dbValidTime.endsNow() && !newValidTime.endsNow()) {
                if (newValidTime.begin().isBefore(dbValidTime.begin())) {
                    resultIntersection = TimeExtent.period(newValidTime.begin(), dbValidTime.begin());
                } else {
                    resultIntersection = TimeExtent.period(dbValidTime.begin(), newValidTime.begin());
                }
                this.updateTime(entry.getKey(), resultIntersection);
            } else if (newValidTime.endsNow() || dbValidTime.endsNow()) {
                // close dbValidTime at the start of newValidTime
                if (newValidTime.begin().isBefore(dbValidTime.begin())) {
                    throw new DataStoreException("Cannot overlap completely the validTime");
                }
                // otherwise, close the dbValidTime at the start of the new time
                resultIntersection = TimeExtent.period(dbValidTime.begin(), newValidTime.begin());
                this.updateTime(entry.getKey(), resultIntersection);
            }
        }
    }

    protected void updateTime(DataStreamKey dataStreamKey, TimeExtent timeExtent) {
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.updateValidTimeByIdQuery())) {
                PGobject jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(SerializerUtils.writeTimeExtent(timeExtent));

                preparedStatement.setObject(1, jsonObject);
                preparedStatement.setLong(2, dataStreamKey.getInternalID().getIdAsLong());
                int rows = preparedStatement.executeUpdate();
                if (rows > 0) {
                    // update cache by invalidate old entry
                    cache.invalidate(dataStreamKey.getInternalID().getIdAsLong());
                } else {
                    throw new RuntimeException("Cannot update datastream ");
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataStreamKey add(IDataStreamInfo dsInfo) throws DataStoreException {
        // RULE --
        // if DS already exists, compute the time intersection
        this.intersectsAndUpdate(dsInfo);

        // 2 cases, if exists, update time with intersection and insert the new one
        // otherwise only create it

        try (Connection connection = connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    queryBuilder.insertInfoQuery(), Statement.RETURN_GENERATED_KEYS)) {
                PGobject jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(SerializerUtils.writeIDataStreamInfoToJson(dsInfo));

                preparedStatement.setObject(1, jsonObject);
                int rows = preparedStatement.executeUpdate();
                if (rows > 0) {
                    try (ResultSet rs = preparedStatement.getGeneratedKeys()) {
                        long generatedKey = 0;
                        if (rs.next()) {
                            generatedKey = rs.getLong(1);
                        }
                        return new DataStreamKey(BigId.fromLong(obsStore.idScope, generatedKey));
                    }
                } else {
                    throw new DataStoreException("Cannot insert dataStreamInfo " + dsInfo.getOutputName());
                }
            }
        } catch (SQLException | IOException e) {
            throw new DataStoreException("Cannot insert dataStreamInfo " + dsInfo.getOutputName(), e);
        }
    }

    @Override
    public IDataStreamInfo get(Object o) {
        if (!(o instanceof DataStreamKey)) {
            throw new UnsupportedOperationException("Get operation is not supported with argument != DataStream key, got=" + o.getClass());
        }
        DataStreamKey key = (DataStreamKey) o;
        long id = key.getInternalID().getIdAsLong();
        IDataStreamInfo dataStreamInfo = cache.getIfPresent(id);
        if (dataStreamInfo == null) {
            lock.lock();
            try {
                // double lock checking + volatile
                dataStreamInfo = cache.getIfPresent(id);
                if (dataStreamInfo == null) {
                    try (Connection connection = connectionManager.getConnection()) {
                        try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.selectByIdQuery())) {
                            preparedStatement.setLong(1, id);
                            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                                if (resultSet.next()) {
                                    String data = resultSet.getString("data");
                                    dataStreamInfo = SerializerUtils.readIDataStreamInfoFromJson(data);
                                    // TODO Fetch result and phenomenon time intervals here to attach to wrapper
                                    IDataStreamInfo wrapper = new DataStreamInfoWithTimeRanges(id, dataStreamInfo);
                                    cache.put(id, wrapper);
                                }
                            }
                        }
                    } catch (SQLException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return dataStreamInfo;
    }

    @Override
    public IDataStreamInfo put(DataStreamKey dataStreamKey, IDataStreamInfo iDataStreamInfo) {
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.updateByIdQuery())) {
                String json = SerializerUtils.writeIDataStreamInfoToJson(iDataStreamInfo);
                PGobject jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(json);

                preparedStatement.setObject(1, jsonObject);
                preparedStatement.setLong(2, dataStreamKey.getInternalID().getIdAsLong());
                int rows = preparedStatement.executeUpdate();
                if (rows > 0) {
                    // update cache by invalidate old entry
                    cache.invalidate(dataStreamKey.getInternalID().getIdAsLong());
                    return iDataStreamInfo;
                } else {
                    throw new RuntimeException("Cannot update datastream ");
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IDataStreamInfo remove(Object o) {
        if (!(o instanceof DataStreamKey)) {
            throw new UnsupportedOperationException("Remove operation is not supported with argument != DataStreamKey key, got=" + o.getClass());
        }
        DataStreamKey key = (DataStreamKey) o;
        IDataStreamInfo data = this.get(key);

        logger.debug("Remove Feature with key={}", key.toString());
        if (this.obsStore instanceof OscarTieredObsStore tiered)
            return tiered.withPinnedDataStreamSchema(
                    key.getInternalID().getIdAsLong(), data,
                    () -> removeTieredDataStream(key, data));

        // remove corresponding Obs
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(key.getInternalID())
                .build();
        this.obsStore.removeEntries(filter);
        long remainingObservations = this.obsStore.countMatchingEntries(filter);
        if (remainingObservations != 0) {
            throw new IllegalStateException("Cannot remove datastream " +
                    key.getInternalID() + ": " + remainingObservations +
                    " observation(s) remain, including records protected by retention holds");
        }

        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.removeByIdQuery())) {
                preparedStatement.setLong(1, key.getInternalID().getIdAsLong());
                int rows = preparedStatement.executeUpdate();
                if (rows > 0) {
                    cache.invalidate(key.getInternalID().getIdAsLong());
                    return data;
                } else {
                    throw new RuntimeException("Cannot remove IDataStreamInfo " + data.getOutputName());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Keeps a SHARE table lock from the hold preflight through metadata
     * deletion. Hold creation/release takes SHARE ROW EXCLUSIVE, so a held
     * child is discovered before any observation is changed and a new hold
     * cannot race between the preflight and the bounded child deletions.
     */
    private IDataStreamInfo removeTieredDataStream(DataStreamKey key, IDataStreamInfo data) {
        long dataStreamId = key.getInternalID().getIdAsLong();
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(key.getInternalID())
                .build();
        try (Connection connection = this.connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                try (Statement lockHolds = connection.createStatement()) {
                    lockHolds.execute("LOCK TABLE oscar_retention_hold IN SHARE MODE");
                }
                long blockingHolds = countActiveRetentionHoldsForDataStream(
                        connection, dataStreamId);
                if (blockingHolds != 0) {
                    throw new IllegalStateException("Cannot remove datastream " +
                            key.getInternalID() + ": " + blockingHolds +
                            " active retention hold(s) protect matching or unresolved observations; " +
                            "no observation was removed");
                }

                this.obsStore.removeEntries(filter);
                long remainingObservations = this.obsStore.countMatchingEntries(filter);
                if (remainingObservations != 0) {
                    throw new IllegalStateException("Cannot remove datastream " +
                            key.getInternalID() + ": " + remainingObservations +
                            " observation(s) remain after hold-aware removal");
                }

                try (PreparedStatement preparedStatement =
                             connection.prepareStatement(queryBuilder.removeByIdQuery())) {
                    preparedStatement.setLong(1, dataStreamId);
                    if (preparedStatement.executeUpdate() != 1)
                        throw new IllegalStateException("Cannot remove IDataStreamInfo " +
                                (data == null ? key.getInternalID() : data.getOutputName()));
                }
                connection.commit();
                cache.invalidate(dataStreamId);
                return data;
            } catch (Exception error) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackError) {
                    error.addSuppressed(rollbackError);
                }
                if (error instanceof RuntimeException runtime)
                    throw runtime;
                throw new IllegalStateException("Cannot remove tiered datastream " +
                        key.getInternalID(), error);
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot remove tiered datastream " +
                    key.getInternalID(), error);
        }
    }

    /**
     * Resolves active exact and conservative holds against the target
     * datastream. An unresolvable legacy/corrupt hold blocks every datastream
     * deletion in its namespace rather than allowing a destructive false
     * negative. Cold observation holds use generation ID ranges plus schema
     * membership and are therefore intentionally conservative.
     */
    private long countActiveRetentionHoldsForDataStream(Connection connection,
                                                        long dataStreamId)
            throws SQLException {
        String observations = this.obsStore.getDatastoreName();
        String sql = "WITH active_holds AS (" +
                "SELECT id, subject_type, subject_id, resolution_mode " +
                "FROM oscar_retention_hold WHERE store_key=? AND id_scope=? " +
                "AND released_at IS NULL), " +
                "exact_resolution(hold_id, datastream_id) AS (" +
                "SELECT hold.id, observation.datastreamid FROM active_holds hold " +
                "JOIN " + observations + " observation ON hold.subject_type='OBSERVATION' " +
                "AND hold.resolution_mode IN ('HOT_EXACT','LEGACY_UNKNOWN') " +
                "AND observation.id::text=hold.subject_id " +
                "UNION SELECT hold.id, overlay_row.datastream_id FROM active_holds hold " +
                "JOIN oscar_obs_overlay overlay_row ON hold.subject_type='OBSERVATION' " +
                "AND hold.resolution_mode IN ('HOT_EXACT','LEGACY_UNKNOWN') " +
                "AND overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND overlay_row.observation_id::text=hold.subject_id " +
                "JOIN oscar_archive_generation overlay_generation " +
                "ON overlay_generation.id=overlay_row.generation_id " +
                "AND overlay_generation.store_key=overlay_row.store_key " +
                "AND overlay_generation.id_scope=overlay_row.id_scope " +
                "AND overlay_generation.state IN ('ACTIVE','SEALED')), " +
                "range_resolution(hold_id, datastream_id) AS (" +
                "SELECT hold.id, schema_row.datastream_id FROM active_holds hold " +
                "JOIN oscar_archive_id_block block ON hold.subject_type='OBSERVATION' " +
                "AND hold.resolution_mode IN ('COLD_RANGE_CONSERVATIVE','LEGACY_UNKNOWN') " +
                "AND CASE WHEN hold.subject_id ~ '^[0-9]+$' " +
                "THEN hold.subject_id::numeric END BETWEEN block.min_id::numeric AND block.max_id::numeric " +
                "JOIN oscar_archive_generation generation ON generation.id=block.generation_id " +
                "AND generation.store_key=? AND generation.id_scope=? " +
                "AND generation.state IN ('ACTIVE','SEALED') " +
                "JOIN oscar_archive_schema schema_row ON schema_row.generation_id=generation.id), " +
                "generation_resolution(hold_id, datastream_id) AS (" +
                "SELECT hold.id, schema_row.datastream_id FROM active_holds hold " +
                "JOIN oscar_archive_generation generation " +
                "ON hold.subject_type='ARCHIVE_GENERATION' " +
                "AND hold.resolution_mode='GENERATION_EXACT' " +
                "AND generation.id::text=hold.subject_id AND generation.store_key=? " +
                "AND generation.id_scope=? AND generation.state IN ('ACTIVE','SEALED') " +
                "JOIN oscar_archive_schema schema_row ON schema_row.generation_id=generation.id), " +
                "resolved(hold_id, datastream_id) AS (" +
                "SELECT exact.hold_id, exact.datastream_id FROM exact_resolution exact " +
                "JOIN active_holds hold ON hold.id=exact.hold_id " +
                "WHERE hold.resolution_mode='HOT_EXACT' " +
                "UNION SELECT range_row.hold_id, range_row.datastream_id " +
                "FROM range_resolution range_row JOIN active_holds hold ON hold.id=range_row.hold_id " +
                "WHERE hold.resolution_mode='COLD_RANGE_CONSERVATIVE' " +
                "UNION SELECT generation_row.hold_id, generation_row.datastream_id " +
                "FROM generation_resolution generation_row " +
                "UNION SELECT exact.hold_id, exact.datastream_id FROM exact_resolution exact " +
                "JOIN active_holds hold ON hold.id=exact.hold_id " +
                "WHERE hold.resolution_mode='LEGACY_UNKNOWN' AND 1=(" +
                "SELECT count(DISTINCT candidate.datastream_id) FROM exact_resolution candidate " +
                "WHERE candidate.hold_id=hold.id) " +
                "UNION SELECT range_row.hold_id, range_row.datastream_id " +
                "FROM range_resolution range_row JOIN active_holds hold ON hold.id=range_row.hold_id " +
                "WHERE hold.resolution_mode='LEGACY_UNKNOWN' " +
                "AND NOT EXISTS (SELECT 1 FROM exact_resolution exact " +
                "WHERE exact.hold_id=hold.id) AND 1=(" +
                "SELECT count(DISTINCT candidate.datastream_id) FROM range_resolution candidate " +
                "WHERE candidate.hold_id=hold.id)), " +
                "resolution_stats AS (SELECT hold_id, count(DISTINCT datastream_id) AS stream_count " +
                "FROM resolved GROUP BY hold_id) " +
                "SELECT count(*) FROM active_holds hold " +
                "LEFT JOIN resolution_stats stats ON stats.hold_id=hold.id WHERE " +
                "stats.stream_count IS NULL OR stats.stream_count=0 OR " +
                "(hold.resolution_mode='HOT_EXACT' AND stats.stream_count<>1) OR " +
                "EXISTS (SELECT 1 FROM resolved WHERE hold_id=hold.id AND datastream_id=?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, observations);
            statement.setInt(parameter++, this.obsStore.idScope);
            statement.setString(parameter++, observations);
            statement.setInt(parameter++, this.obsStore.idScope);
            statement.setString(parameter++, observations);
            statement.setInt(parameter++, this.obsStore.idScope);
            statement.setString(parameter++, observations);
            statement.setInt(parameter++, this.obsStore.idScope);
            statement.setLong(parameter, dataStreamId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    throw new SQLException("Retention-hold preflight returned no result");
                return result.getLong(1);
            }
        }
    }

    @Override
    public void clear() {
        if (obsStore instanceof OscarTieredObsStore) {
            throw new UnsupportedOperationException(
                    "Tiered datastream metadata cannot be cleared independently of held observations");
        }
        try {
            super.clear();
            this.clearCache();
        } catch (Exception e) {
            throw e;
        } finally {
            cache.invalidateAll();
        }
    }

    @Override
    public void drop() {
        if (obsStore instanceof OscarTieredObsStore) {
            throw new UnsupportedOperationException(
                    "Tiered datastream metadata cannot be dropped through the datastore API");
        }
        super.drop();
    }

    @Override
    public void backup(java.io.OutputStream output) throws IOException {
        if (obsStore instanceof OscarTieredObsStore) {
            throw new UnsupportedOperationException(
                    "Tiered datastream metadata requires the coordinated OSCAR storage backup");
        }
        super.backup(output);
    }

    @Override
    public void restore(java.io.InputStream input) throws IOException {
        if (obsStore instanceof OscarTieredObsStore) {
            throw new UnsupportedOperationException(
                    "Tiered datastream metadata requires the coordinated OSCAR storage restore");
        }
        super.restore(input);
    }

    public void clearCache() {
        cache.invalidateAll();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.get(key) != null;
    }


    @Override
    public Set<Map.Entry<DataStreamKey, IDataStreamInfo>> entrySet() {
        DataStreamFilter dataStreamFilter = new DataStreamFilter.Builder().build();
        return this.selectEntries(dataStreamFilter, new HashSet<>()).collect(Collectors.toSet());
    }

    public Collection<IDataStreamInfo> values() {
        return this.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toSet());
    }

    public Set<DataStreamKey> keySet() {
        return this.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    @Override
    public void linkTo(ISystemDescStore systemStore) {
        super.linkTo(systemStore);
    }

    public Set<Long> getDataStreamsIdsByTimeRange(Instant min, Instant max) {
        Set<Long> results = new LinkedHashSet<>();
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(queryBuilder.getAllDataStreams())) {
                statement.setString(1, "[" + PostgisUtils.checkAndGetValidInstant(min) + "," + PostgisUtils.checkAndGetValidInstant(max) + "]");
                logger.debug(queryBuilder.getAllDataStreams());
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next())
                        results.add(resultSet.getLong("id"));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

    @Override
    protected void initUidHashIdProvider() {
        idProvider = DataStoreUtils.getDataStreamHashIdProvider(741532149);
    }

}
