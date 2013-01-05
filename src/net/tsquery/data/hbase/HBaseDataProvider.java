/*
 * Copyright 2011 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * removed extraneous code
 *
 * @author kevin ortman
 *
 */
package net.tsquery.data.hbase;

import net.tsquery.data.TsdbDataProvider;
import net.tsquery.model.DataPoint;
import net.tsquery.model.ID;
import net.tsquery.model.Metric;
import net.tsquery.model.TagsArray;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class HBaseDataProvider implements TsdbDataProvider {

    protected static Logger logger = Logger
            .getLogger("com.facebook.tsdb.services");

    public static final String ID_FAMILY = "id";

    public static final String METRIC_QUALIFIER = "metrics";
    public static final String TAG_QUALIFIER = "tagk";
    public static final String TAG_VALUE_QUALIFIER = "tagv";

    private final HTable dataTable;
    private final IDMap idMap = new IDMap();

    public HBaseDataProvider() throws IOException {
        dataTable = HBaseConnection.getDataTableConn();
    }

    @Override
    public Metric fetchMetricHeader(String metric, long startTs, long toTs,
            Map<String, String> tags) throws Exception {
        ID metricID = idMap.getMetricID(metric);
        Metric metricData = new Metric(metricID.id, metric);
        RowRange rowRange = new RowRange(metricID.id, startTs, toTs);

        Scan scan = new Scan(rowRange.getStart(), rowRange.getStop());

        ResultScanner scanner = dataTable.getScanner(scan);
        int count = 0;
        for (Result result : scanner) {
            RowKey rowKey = new RowKey(result.getRow(), idMap);
            TagsArray rowTags = rowKey.getTags(TagsArray.NATURAL_ORDER);
            if (!metricData.timeSeries.containsKey(rowTags)) {
                metricData.timeSeries.put(rowTags, new ArrayList<DataPoint>());
            }
            count++;
        }
        logger.info("Fetching header for " + metric + ": " + count + " rows. ");
        return metricData;
    }

    @Override
    public String[] getMetrics() throws Exception {
        return idMap.getMetrics();
    }

}
