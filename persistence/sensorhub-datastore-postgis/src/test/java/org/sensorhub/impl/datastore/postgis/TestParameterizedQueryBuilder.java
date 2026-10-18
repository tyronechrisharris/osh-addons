/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis;

import com.google.common.collect.Range;
import org.junit.Test;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderCommandStreamStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderDataStreamStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderSystemDescStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.Set;

public class TestParameterizedQueryBuilder {
    private static final String INJECTION = "camera') OR 1=1 --";

    @Test
    public void testDataStreamFullTextFilterIsParameterized() {
        DataStreamFilter filter = new DataStreamFilter.Builder()
                .withKeywords(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderDataStreamStore("datastreams")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("~* ?"));
        assertEquals("(" + INJECTION + ")", query.parameters().get(0));
    }

    @Test
    public void testCommandStreamFullTextFilterIsParameterized() {
        CommandStreamFilter filter = new CommandStreamFilter.Builder()
                .withKeywords(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderCommandStreamStore("commandstreams")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("~* ?"));
        assertEquals("(" + INJECTION + ")", query.parameters().get(0));
    }

    @Test
    public void testSystemUidFilterIsParameterized() {
        SystemFilter filter = new SystemFilter.Builder()
                .withUniqueIDs(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderSystemDescStore("systems")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("ILIKE ?"));
        assertEquals("%" + INJECTION, query.parameters().get(0));
    }

    @Test
    public void testDataStreamOutputNameIsParameterized() {
        DataStreamFilter filter = new DataStreamFilter.Builder()
                .withOutputNames(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderDataStreamStore("datastreams")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("in (?)"));
        assertEquals(INJECTION, query.parameters().get(0));
    }

    @Test
    public void testObsCqlStringLiteralIsParameterized() {
        ObsFilter filter = new ObsFilter.Builder()
                .withCQLFilter("name = '" + INJECTION.replace("'", "''") + "'")
                .build();

        ParameterizedQuery query = new QueryBuilderObsStore("obs")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("result @> ?::jsonb"));
        assertTrue(query.parameters().get(0).toString().contains(INJECTION));
    }

    @Test
    public void testObservationTimeBoundsArePreservedForSelectAndRemove() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        Instant end = Instant.parse("2026-01-02T00:00:00Z");
        ObsFilter halfOpen = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.closedOpen(start, end))
                        .build())
                .build();
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");

        ParameterizedQuery select = builder.createParameterizedSelectEntriesQuery(halfOpen, null);
        ParameterizedQuery remove = builder.createParameterizedRemoveEntriesQuery(halfOpen);

        for (ParameterizedQuery query : Arrays.asList(select, remove)) {
            assertTrue(query.sql().contains("obs.phenomenonTime >= ?::timestamp"));
            assertTrue(query.sql().contains("obs.phenomenonTime < ?::timestamp"));
            assertFalse(query.sql().contains("obs.phenomenonTime <= ?::timestamp"));
            assertEquals(2, query.parameters().size());
        }

        ObsFilter inverse = new ObsFilter.Builder()
                .withResultTime(new TemporalFilter.Builder()
                        .withRange(Range.openClosed(start, end))
                        .build())
                .build();
        ParameterizedQuery inverseRemove = builder.createParameterizedRemoveEntriesQuery(inverse);
        assertTrue(inverseRemove.sql().contains("obs.resultTime > ?::timestamp"));
        assertTrue(inverseRemove.sql().contains("obs.resultTime <= ?::timestamp"));
    }

    @Test
    public void testObsEventFilterIndexesMatchGeneratedPredicates() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        String indexes = String.join("\n", builder.createEventFilterIndexQueries());

        assertTrue(indexes.contains("USING GIN(result jsonb_path_ops)"));
        assertTrue(indexes.contains("(dataStreamID, ((result->>'occupancyCount')::numeric))"));
        assertTrue(indexes.contains("(dataStreamID, ((result->>'maxGamma')::numeric))"));
        assertTrue(indexes.contains("(dataStreamID, ((result->>'maxNeutron')::numeric))"));
        assertTrue(indexes.contains("WHERE jsonb_typeof(result->'occupancyCount') = 'number'"));
        assertTrue(indexes.contains("WHERE jsonb_typeof(result->'maxGamma') = 'number'"));
        assertTrue(indexes.contains("WHERE jsonb_typeof(result->'maxNeutron') = 'number'"));
        assertTrue(Arrays.stream(builder.createEventFilterIndexQueries())
                .allMatch(query -> query.startsWith("CREATE INDEX IF NOT EXISTS")));
    }

    @Test
    public void testObservationInsertUsesStableIdempotentNaturalKeyUpsert() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");

        for (String query : Arrays.asList(builder.insertObsQuery(), builder.insertObsPreparedQuery())) {
            assertTrue(query.startsWith("INSERT INTO obs"));
            assertTrue(query.contains("ON CONFLICT (dataStreamID, COALESCE(foiID, -1::bigint)"));
            assertTrue(query.contains("DO UPDATE SET result=EXCLUDED.result"));
            assertTrue(query.endsWith("WHERE obs.result IS DISTINCT FROM EXCLUDED.result"));
            assertFalse(query.contains("SET id="));
        }

        String naturalKeyLookup = builder.findByUniqueFieldsQuery();
        assertTrue(naturalKeyLookup.contains("dataStreamID = ?"));
        assertTrue(naturalKeyLookup.contains("foiID IS NOT DISTINCT FROM ?"));
        assertTrue(naturalKeyLookup.contains("phenomenonTime IS NOT DISTINCT FROM ?::timestamp"));
        assertTrue(naturalKeyLookup.contains("resultTime IS NOT DISTINCT FROM ?::timestamp"));
    }

    @Test
    public void testLatestResultsForExplicitDataStreamsUseParameterizedLateralProbes() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(
                        BigId.fromLong(2, 101),
                        BigId.fromLong(2, 202),
                        BigId.fromLong(2, 303))
                .withLatestResult()
                .withLimit(2)
                .build();

        assertTrue(builder.supportsLatestResultPerDataStreamQuery(filter));

        ParameterizedQuery query = builder.createParameterizedLatestResultPerDataStreamQuery(
                filter,
                Set.of(IObsStore.ObsField.PHENOMENON_TIME, IObsStore.ObsField.RESULT));

        assertTrue(query.sql().contains("unnest(ARRAY[?,?,?]::bigint[])"));
        assertTrue(query.sql().contains("CROSS JOIN LATERAL"));
        assertTrue(query.sql().contains("candidate.dataStreamID = requested.dataStreamID"));
        assertTrue(query.sql().contains("ORDER BY candidate.resultTime DESC, candidate.phenomenonTime ASC FETCH FIRST 1 ROW ONLY"));
        assertTrue(query.sql().endsWith("ORDER BY latest.dataStreamID, latest.resultTime DESC, latest.phenomenonTime ASC LIMIT 2"));
        assertTrue(query.sql().startsWith("SELECT latest.phenomenonTime,latest.result,latest.id"));
        assertEquals(Arrays.asList(101L, 202L, 303L), query.parameters());
    }

    @Test
    public void testLatestResultOptimizationFallsBackForBroaderFilters() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        BigId dataStreamId = BigId.fromLong(2, 101);

        assertFalse(builder.supportsLatestResultPerDataStreamQuery(new ObsFilter.Builder()
                .withDataStreams(dataStreamId)
                .build()));
        assertFalse(builder.supportsLatestResultPerDataStreamQuery(new ObsFilter.Builder()
                .withDataStreams(new DataStreamFilter.Builder()
                        .withOutputNames("connectionStatus")
                        .build())
                .withLatestResult()
                .build()));
        assertFalse(builder.supportsLatestResultPerDataStreamQuery(new ObsFilter.Builder()
                .withDataStreams(dataStreamId)
                .withLatestResult()
                .withCQLFilter("isConnected = true")
                .build()));
    }

    @Test
    public void testLatestResultQueryLeavesUnboundedPagingToIterator() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(2, 101), BigId.fromLong(2, 202))
                .withLatestResult()
                .build();

        ParameterizedQuery query = builder.createParameterizedLatestResultPerDataStreamQuery(filter, null);

        assertTrue(query.sql().contains("FETCH FIRST 1 ROW ONLY"));
        assertFalse(query.sql().contains(" LIMIT "));
    }

    @Test
    public void testLatestResultQueryDeduplicatesLocalIdsAcrossScopes() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(
                        BigId.fromLong(1, 101),
                        BigId.fromLong(2, 101),
                        BigId.fromLong(2, 202))
                .withLatestResult()
                .build();

        ParameterizedQuery query = builder.createParameterizedLatestResultPerDataStreamQuery(filter, null);

        assertTrue(query.sql().contains("unnest(ARRAY[?,?]::bigint[])"));
        assertEquals(Arrays.asList(101L, 202L), query.parameters());
    }

    private void assertSqlDoesNotContainInjection(ParameterizedQuery query) {
        assertFalse(query.sql().contains(INJECTION));
        assertTrue(query.parameters().stream().anyMatch(parameter -> parameter.toString().contains(INJECTION)));
    }
}
