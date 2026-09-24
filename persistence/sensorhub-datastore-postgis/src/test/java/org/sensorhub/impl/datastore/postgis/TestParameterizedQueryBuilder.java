/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis;

import org.junit.Test;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
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

import java.util.Arrays;

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

    private void assertSqlDoesNotContainInjection(ParameterizedQuery query) {
        assertFalse(query.sql().contains(INJECTION));
        assertTrue(query.parameters().stream().anyMatch(parameter -> parameter.toString().contains(INJECTION)));
    }
}
