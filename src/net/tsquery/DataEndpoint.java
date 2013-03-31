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

            int topN = this.getTopN(request, -1);

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
                if(metricQuery.downsample > 0)
                        queries[i].downsample(metricQuery.downsample, _aggregators.get(metricQuery.aggregator));
            }


            long ts = System.currentTimeMillis();
            net.opentsdb.graph.Plot plot = api.Query(_tsdb, tsFrom, tsTo, queries, TimeZone.getDefault(), false);
            responseObj.put("loadtime", System.currentTimeMillis() - ts);

            ts = System.currentTimeMillis();
            responseObj.put("series", PlotToJSON(plot, dygraphOutput, topN));
            responseObj.put("serializationtime", System.currentTimeMillis() - ts);

            doSendResponse(request, out, responseObj.toJSONString());

        } catch (Exception e) {
            out.println(getErrorResponse(e));
        }
        out.close();
    }

    private int getTopN(HttpServletRequest request, int defaultValue) {
        String strval = request.getParameter("topN");
        int value = defaultValue;

        if(strval != null)    {
            try
            {
                value = Integer.parseInt(strval);
            }
            catch(NumberFormatException nfe)
            {
            }
        }

        return value;
    }

    public JSONObject PlotToJSON(Plot plot, boolean dygraphOutput, int topN) {
        if(dygraphOutput) {
            return PlotToDygraphJSON(plot, topN);
        } else {
            return PlotToStandardJSON(plot, topN);
        }

    }

    @SuppressWarnings("unchecked")
    private JSONObject PlotToDygraphJSON(Plot plot, int topN) {
        final JSONObject plotObject = new JSONObject();
        final JSONArray nameArray = new JSONArray();
        final JSONArray dataArray = new JSONArray();
        final int dpCount = plot.getDataPointsSize();

        final TreeMap<Long, double[]> tsMap = new TreeMap<Long, double[]>();
        final double[] weight = new double[dpCount];

        int dpIndex = 0;
        for (DataPoints dataPoints : plot.getDataPoints()) {

            for (DataPoint point : dataPoints) {
                long timestamp = point.timestamp() * 1000;

                if(!tsMap.containsKey(timestamp)) {
                    double[] values = new double[dpCount];
                    values[dpIndex] = getValue(point);
                    tsMap.put(timestamp, values);

                    weight[dpIndex] += ((values[dpIndex]) / 1000000.0);
                }
                else {
                    //noinspection MismatchedReadAndWriteOfArray
                    double[] values = tsMap.get(timestamp);
                    values[dpIndex] = getValue(point);
                    weight[dpIndex] += ((values[dpIndex]) / 1000000.0);
                }
            }

            dpIndex++;
        }

        HashMap<Integer, Boolean> includeMap = null;
        // are we performing a topN lookup?
        if(topN > 0) {
            includeMap = new HashMap<Integer, Boolean>(topN);
            TreeMap<Double, Integer> weightMap = new TreeMap<Double, Integer>(Collections.reverseOrder());
            for(int i=0; i < dpCount; i++){
                while(weightMap.containsKey(weight[i]))
                    weight[i] -= 0.00000001;

                weightMap.put(weight[i], i);
            }

            int series = 0;
            for (Map.Entry<Double, Integer> entry : weightMap.entrySet()) {
                includeMap.put(entry.getValue(), true);

                ++series;
                if(series >= topN)
                    break;
            }
        }

        for(Map.Entry<Long,double[]> entry : tsMap.entrySet()) {
            JSONArray entryArray = new JSONArray();
            entryArray.add(entry.getKey());
            final double[] points = entry.getValue();

            for(dpIndex = 0; dpIndex < dpCount; dpIndex++ ) {
                if((topN <= 0) || (topN > 0 && includeMap.containsKey(dpIndex))) {
                    entryArray.add(points[dpIndex]);
                }
            }

            dataArray.add(entryArray);
        }

        // First column is always the Date
        nameArray.add("Date");

        int index = -1;
        for (DataPoints dataPoints : plot.getDataPoints()) {
            index++;

            // if we are in a topN query and the current index is not included, skip this iteration
            if(topN > 0 && !includeMap.containsKey(index))
                continue;

            StringBuilder nameBuilder = new StringBuilder();

            nameBuilder.append(dataPoints.metricName()).append(":");

            Map<String,String> tags = dataPoints.getTags();
            for (String s : tags.keySet()) {
                nameBuilder.append(String.format(" %s=%s", s, tags.get(s)) );
            }

            nameArray.add(nameBuilder.toString());
        }
        plotObject.put("labels", nameArray);
        plotObject.put("values", dataArray);

        return plotObject;
    }

    private double getValue(DataPoint point) {
        double value;
        if (point.isInteger()) {
            value = (double)point.longValue();
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
    private JSONObject PlotToStandardJSON(Plot plot, int topN) {
        final JSONObject plotObject = new JSONObject();
        JSONArray seriesArray = new JSONArray();

        final TreeMap<Double, JSONObject> weightMap = new TreeMap<Double, JSONObject>(Collections.reverseOrder());

        for (DataPoints dataPoints : plot.getDataPoints()) {
            double weight = 0;
            JSONArray dataArray = new JSONArray();
            StringBuilder nameBuilder = new StringBuilder();

            nameBuilder.append(dataPoints.metricName()).append(": ");

            Map<String,String> tags = dataPoints.getTags();
            for (String s : tags.keySet()) {
                nameBuilder.append(String.format("%s=%s, ", s, tags.get(s)) );
            }
            nameBuilder.setLength(nameBuilder.length() - 2);

            for (DataPoint point : dataPoints) {
                double dpValue = getValue(point);
                JSONArray values = new JSONArray();
                values.add(point.timestamp() * 1000);
                values.add(dpValue);

                weight += ((dpValue) / 1000000.0);

                dataArray.add(values);
            }


            JSONObject series = new JSONObject();
            series.put("name", nameBuilder.toString());
            series.put("data", dataArray);

            while(weightMap.containsKey(weight))
                weight -= 0.00000001;

            weightMap.put(weight, series);
        }

        int counter = 0;
        for (Map.Entry<Double, JSONObject> entry : weightMap.entrySet()) {
            seriesArray.add(entry.getValue());

            ++counter;
            if((topN > 0) && (counter >= topN))
                break;
        }

        plotObject.put("plot", seriesArray);

        return plotObject;
    }

}
