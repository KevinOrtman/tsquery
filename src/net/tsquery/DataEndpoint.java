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

/* Copyright 2013 Kevin Ortman
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

package net.tsquery;

import net.opentsdb.graph.Plot;
import net.tsquery.model.MetricQuery;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.tsd.TsdApi;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class DataEndpoint extends TsdbServlet {

    private static final long serialVersionUID = 1L;
    private static final HashMap<String, Aggregator> _aggregators = new HashMap<String, Aggregator> (5);

    static {
        _aggregators.put("sum", net.opentsdb.core.Aggregators.SUM);
        _aggregators.put("min", net.opentsdb.core.Aggregators.MIN);
        _aggregators.put("max", net.opentsdb.core.Aggregators.MAX);
        _aggregators.put("avg", net.opentsdb.core.Aggregators.AVG);
        _aggregators.put("dev", net.opentsdb.core.Aggregators.DEV);
    }


    @Override
    @SuppressWarnings("unchecked")
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        response.setContentType("application/json");

        TsdApi api = new TsdApi();
        PrintWriter out = response.getWriter();
        try {
            JSONObject responseObj = new JSONObject();
            boolean dygraphOutput = false;

            // decode parameters
            String jsonParams = request.getParameter("params");
            if (jsonParams == null) {
                throw new IllegalArgumentException("Required parameter 'params' not specified");
            }

            String output = request.getParameter("output");
            if(output != null && output.equalsIgnoreCase("dygraph")) {
                dygraphOutput = true;
            }

            JSONObject jsonParamsObj = (JSONObject) JSONValue.parse(jsonParams);
            if(jsonParamsObj == null) {
                throw new IllegalArgumentException("Required parameter 'params' is not a valid JSON object");
            }

            long tsFrom = this.getRequiredTimeStamp(jsonParamsObj, "tsFrom");
            long tsTo = this.getRequiredTimeStamp(jsonParamsObj, "tsTo");

            JSONArray metricsArray = (JSONArray) jsonParamsObj.get("metrics");
            if (metricsArray == null || metricsArray.size() == 0) {
                throw new IllegalArgumentException("Required parameter 'metrics' array not specified or empty");
            }

            Query[] queries = new Query[metricsArray.size()];
            for (int i = 0; i < metricsArray.size(); i++) {
                MetricQuery metricQuery = MetricQuery.fromJSONObject((JSONObject) metricsArray.get(i));

                queries[i] = _tsdb.newQuery();
                queries[i].setTimeSeries(metricQuery.name, metricQuery.tags,
                        _aggregators.get(metricQuery.aggregator), metricQuery.rate);
            }


            long ts = System.currentTimeMillis();
            net.opentsdb.graph.Plot plot = api.Query(_tsdb, tsFrom, tsTo, queries, TimeZone.getDefault(), false);
            responseObj.put("loadtime", System.currentTimeMillis() - ts);

            ts = System.currentTimeMillis();
            responseObj.put("series", PlotToJSON(plot, dygraphOutput));
            responseObj.put("serializationtime", System.currentTimeMillis() - ts);

            doSendResponse(request, out, responseObj.toJSONString());

        } catch (Exception e) {
            out.println(getErrorResponse(e));
        }
        out.close();
    }

    public JSONObject PlotToJSON(Plot plot, boolean dygraphOutput) {
        if(dygraphOutput) {
            return PlotToDygraphJSON(plot);
        } else {
            return PlotToStandardJSON(plot);
        }

    }

    @SuppressWarnings("unchecked")
    private JSONObject PlotToDygraphJSON(Plot plot) {
        JSONObject plotObject = new JSONObject();
        JSONArray dataArray = new JSONArray();
        int dpCount = 0;

        JSONArray nameArray = new JSONArray();
        nameArray.add("Date");
        for (DataPoints dataPoints : plot.getDataPoints()) {
            StringBuilder nameBuilder = new StringBuilder();

            nameBuilder.append(dataPoints.metricName()).append(":");

            Map<String,String> tags = dataPoints.getTags();
            for (String s : tags.keySet()) {
                nameBuilder.append(String.format(" %s=%s", s, tags.get(s)) );
            }

            nameArray.add(nameBuilder.toString());
            dpCount++;
        }
        plotObject.put("labels", dataArray);

        TreeMap<Long, Object[]> tsMap = new TreeMap<Long, Object[]>();
        int dpIndex = 0;
        for (DataPoints dataPoints : plot.getDataPoints()) {

            for (DataPoint point : dataPoints) {
                long timestamp = point.timestamp() * 1000;

                if(!tsMap.containsKey(timestamp)) {
                    Object[] values = new Object[dpCount];
                    values[dpIndex] = getValue(point);
                    tsMap.put(timestamp, values);
                }
                else {
                    //noinspection MismatchedReadAndWriteOfArray
                    Object[] values = tsMap.get(timestamp);
                    values[dpIndex] = getValue(point);
                }
            }

            dpIndex++;
        }

        for(Map.Entry<Long,Object[]> entry : tsMap.entrySet()) {
            JSONArray entryArray = new JSONArray();
            entryArray.add(entry.getKey());
            Object[] points = entry.getValue();

            for(dpIndex = 0; dpIndex < dpCount; dpIndex++ ) {
                entryArray.add(points[dpIndex]);
            }

            dataArray.add(entryArray);
        }

        plotObject.put("values", dataArray);

        return plotObject;
    }

    private Object getValue(DataPoint point) {
        Object value;
        if (point.isInteger()) {
            value = point.longValue();
        } else {
            final double doubleValue = point.doubleValue();
            if (doubleValue != doubleValue || Double.isInfinite(doubleValue)) {
                throw new IllegalStateException("invalid datapoint found");
            }
            value = doubleValue;
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    private JSONObject PlotToStandardJSON(Plot plot) {
        JSONObject plotObject = new JSONObject();
        JSONArray seriesArray = new JSONArray();

        for (DataPoints dataPoints : plot.getDataPoints()) {
            JSONArray dataArray = new JSONArray();
            StringBuilder nameBuilder = new StringBuilder();

            nameBuilder.append(dataPoints.metricName()).append(": ");

            Map<String,String> tags = dataPoints.getTags();
            for (String s : tags.keySet()) {
                nameBuilder.append(String.format("%s=%s, ", s, tags.get(s)) );
            }
            nameBuilder.setLength(nameBuilder.length() - 2);

            for (DataPoint point : dataPoints) {
                JSONArray values = new JSONArray();
                values.add(point.timestamp() * 1000);
                values.add(getValue(point));

                dataArray.add(values);
            }

            JSONObject series = new JSONObject();
            series.put("name", nameBuilder.toString());
            series.put("data", dataArray);

            seriesArray.add(series);
        }

        plotObject.put("plot", seriesArray);

        return plotObject;
    }

}
