/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2023 Georobotix. All Rights Reserved.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.builder.filter.obs;

import com.google.common.collect.BoundType;
import org.geotools.api.filter.Filter;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.postgis.builder.filter.CQLFilterHandler;
import org.sensorhub.impl.datastore.postgis.builder.filter.datastream.DataStreamFilterQuery;
import org.sensorhub.impl.datastore.postgis.builder.filter.datastream.SelectDataStreamFilterQuery;
import org.sensorhub.impl.datastore.postgis.builder.filter.feature.SelectFoiFilterQuery;
import org.sensorhub.impl.datastore.postgis.builder.generator.SelectFilterQueryGenerator;
import org.sensorhub.impl.datastore.postgis.utils.PostgisUtils;
import org.vast.util.Asserts;

import java.util.stream.Collectors;

import static org.sensorhub.api.datastore.obs.IObsStore.ObsField.DATASTREAM_ID;
import static org.sensorhub.api.datastore.obs.IObsStore.ObsField.FOI_ID;

public class SelectObsFilterQuery extends BaseObsFilterQuery<SelectFilterQueryGenerator> {

    CQLFilterHandler cqlHandler;

    public SelectObsFilterQuery(String tableName, SelectFilterQueryGenerator filterQueryGenerator) {
        super(tableName, filterQueryGenerator);
        cqlHandler = new CQLFilterHandler();
    }

    public SelectFilterQueryGenerator build(ObsFilter filter) {
        this.filterQueryGenerator = super.build(filter);
        this.handleSorted(filter.getPhenomenonTime() != null && filter.getPhenomenonTime().descendingOrder());
        return this.filterQueryGenerator;
    }

    @Override
    protected void handleCQLFilter(Filter filter) {
        var whereClause = cqlHandler.buildWhereClause(filter);
        if (whereClause == null || whereClause.isBlank())
            return;

        filterQueryGenerator.addCondition(whereClause);
        filterQueryGenerator.addParameters(cqlHandler.getParameters());
    }

    protected void handleSorted(boolean descending) {
        StringBuilder sb = new StringBuilder(this.tableName)
                .append(".phenomenonTime ")
                .append(descending ? "DESC" : "ASC")
                .append(" NULLS FIRST");
        this.filterQueryGenerator.addOrderBy(sb.toString());
        // Stable tie-breaking is required by the tiered hot/cold merge and
        // also prevents OFFSET pagination from skipping equal-time rows.
        this.filterQueryGenerator.addOrderBy(this.tableName + ".id ASC");
    }

    protected void handleDataStreamFilter(DataStreamFilter dataStreamFilter) {
        if (dataStreamFilter != null) {
            // To avoid JOIN if we have only datastreamid to check
            if(DataStreamFilterQuery.hasOnlyInternalIds(dataStreamFilter)) {
                String operator = "IN";
                if(dataStreamFilter.getInternalIDs().size() == 1) {
                    operator = "=";
                }

                this.filterQueryGenerator.addCondition(this.tableName + "." + DATASTREAM_ID + " "+operator+" ("+
                        placeholders(dataStreamFilter.getInternalIDs().size()) + ")");
                dataStreamFilter.getInternalIDs().forEach(bigId -> addParameter(bigId.getIdAsLong()));
            } else {
                // create join
                Asserts.checkNotNull(dataStreamTableName, "dataStreamTableName should not be null");

                this.filterQueryGenerator.addPrioritizedInnerJoin(this.dataStreamTableName + " ON " + this.tableName + ".datastreamid = " + this.dataStreamTableName + ".id");
                SelectDataStreamFilterQuery dataStreamFilterQuery = new SelectDataStreamFilterQuery(this.dataStreamTableName, filterQueryGenerator);
                dataStreamFilterQuery.setSysDescTableName(this.sysDescTableName);
                this.filterQueryGenerator = dataStreamFilterQuery.build(dataStreamFilter);
            }
        }
    }

    protected void handlePhenomenonTimeFilter(TemporalFilter temporalFilter) {
        if (temporalFilter != null) {
            if (temporalFilter.isLatestTime()) {
                filterQueryGenerator.addDistinct(this.tableName + ".datastreamid");
                filterQueryGenerator.addOrderBy(this.tableName + ".datastreamid");
                filterQueryGenerator.addOrderBy(this.tableName + ".phenomenonTime DESC");
            } else {
                String min = PostgisUtils.checkAndGetValidInstant(temporalFilter.getMin());
                String max = PostgisUtils.checkAndGetValidInstant(temporalFilter.getMax());
                addTimeRangeCondition(this.tableName + ".phenomenonTime", temporalFilter, min, max);
            }
        }
    }

    protected void handleResultTimeFilter(TemporalFilter temporalFilter) {
        if (temporalFilter != null) {
            if (temporalFilter.isLatestTime()) {
                filterQueryGenerator.addDistinct(this.tableName + ".datastreamid");
                filterQueryGenerator.addOrderBy(this.tableName + ".datastreamid");
                filterQueryGenerator.addOrderBy(this.tableName + ".resultTime DESC");
            } else {
                String min = PostgisUtils.checkAndGetValidInstant(temporalFilter.getMin());
                String max = PostgisUtils.checkAndGetValidInstant(temporalFilter.getMax());
                addTimeRangeCondition(this.tableName + ".resultTime", temporalFilter, min, max);
            }
        }
    }

    /** Adds bound-aware comparisons instead of tsrange containment for B-tree index use. */
    private void addTimeRangeCondition(String columnName, TemporalFilter temporalFilter,
                                       String min, String max) {
        boolean hasMin = isValidBound(min);
        boolean hasMax = isValidBound(max);
        String lowerOperator = temporalFilter.getRange().lowerBoundType() == BoundType.CLOSED ? ">=" : ">";
        String upperOperator = temporalFilter.getRange().upperBoundType() == BoundType.CLOSED ? "<=" : "<";

        if (hasMin && hasMax) {
            addCondition(columnName + " " + lowerOperator + " ?::timestamp");
            addParameter(min);
            addCondition(columnName + " " + upperOperator + " ?::timestamp");
            addParameter(max);
        } else if (hasMin) {
            addCondition(columnName + " " + lowerOperator + " ?::timestamp");
            addParameter(min);
        } else if (hasMax) {
            addCondition(columnName + " " + upperOperator + " ?::timestamp");
            addParameter(max);
        }
        // If neither bound is valid, no condition is added (matches all times)
    }

    private boolean isValidBound(String bound) {
        if (bound == null || bound.isEmpty()) {
            return false;
        }
        String lower = bound.toLowerCase().trim();
        return !lower.equals("-infinity") && !lower.equals("infinity");
    }

    protected void handleFoiFilter(FoiFilter foiFilter, ObsFilter obsFilter) {
        if (foiFilter != null) {
            if (this.foiTableName != null) {
                // create JOIN
                Asserts.checkNotNull(foiFilter, "foiFilter should not be null");

                this.filterQueryGenerator.addInnerJoin(this.foiTableName + " ON " + this.tableName + ".foiid = " + this.foiTableName + ".id");
                SelectFoiFilterQuery foiFilterQuery = new SelectFoiFilterQuery(this.foiTableName, filterQueryGenerator);
                foiFilterQuery.setFoiTableName(this.foiTableName);

                this.filterQueryGenerator = foiFilterQuery.build(foiFilter);
            } else {
                // otherwise
                if (foiFilter.getInternalIDs() != null || foiFilter.getUniqueIDs() != null) {
                    // handle Internal IDs
                    if(foiFilter.getInternalIDs() != null) {
                        if (foiFilter.getInternalIDs().contains(BigId.NONE)) {
                            addCondition(this.tableName + "." + FOI_ID + " IS NULL");
                        } else {
                            String operator = "IN";
                            if(foiFilter.getInternalIDs().size() == 1) {
                                operator = "=";
                            }
                            addCondition(this.tableName + "." + FOI_ID + " "+operator+" (" +
                                    placeholders(foiFilter.getInternalIDs().size()) + ")");
                            foiFilter.getInternalIDs().forEach(bigId -> addParameter(bigId.getIdAsLong()));
                        }
                    }
                }
                if (foiFilter.getParentFilter() != null || foiFilter.getObservationFilter() != null ||
                foiFilter.getLocationFilter() != null || foiFilter.getSampledFeatureFilter() != null ||
                foiFilter.getFullTextFilter() != null || foiFilter.getValidTime() != null)  {
                    throw new IllegalStateException("No linked foi store");
                }
            }
        }
    }

}
