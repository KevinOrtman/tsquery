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
package com.facebook.tsdb.tsdash.server;

import com.facebook.tsdb.tsdash.server.model.MetricQuery;
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
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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

            // decode parameters
            String jsonParams = request.getParameter("params");
            if (jsonParams == null) {
                throw new Exception("Parameters not specified");
            }

            JSONObject jsonParamsObj = (JSONObject) JSONValue.parse(jsonParams);
            long tsFrom = (Long) jsonParamsObj.get("tsFrom");
            long tsTo = (Long) jsonParamsObj.get("tsTo");
            JSONArray metricsArray = (JSONArray) jsonParamsObj.get("metrics");
            if (metricsArray.size() == 0) {
                throw new Exception("No metrics to fetch");
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
            responseObj.put("series", PlotToJSONArray(plot));
            responseObj.put("serializationtime", System.currentTimeMillis() - ts);

            doSendResponse(request, out, responseObj.toJSONString());

        } catch (Exception e) {
            out.println(getErrorResponse(e));
        }
        out.close();
    }

    @SuppressWarnings("unchecked")
    public JSONArray PlotToJSONArray(net.opentsdb.graph.Plot plot) {
        JSONArray seriesArray = new JSONArray();

        for (DataPoints dataPoints : plot.getDataPoints()) {
            JSONArray dataArray = new JSONArray();
            StringBuilder nameBuilder = new StringBuilder();

            Map<String,String> tags = dataPoints.getTags();
            for (String s : tags.keySet()) {
                nameBuilder.append(String.format("%s=%s, ", s, tags.get(s)) );
            }
            nameBuilder.setLength(nameBuilder.length() - 2);

            for (DataPoint point : dataPoints) {
                JSONArray values = new JSONArray();
                values.add(point.timestamp() * 1000);
                if (point.isInteger()) {
                    values.add(point.longValue());
                } else {
                    final double value = point.doubleValue();
                    if (value != value || Double.isInfinite(value)) {
                        throw new IllegalStateException("invalid datapoint found");
                    }
                    values.add(value);
                }
                dataArray.add(values);
            }

            JSONObject series = new JSONObject();
            series.put("name", nameBuilder.toString());
            series.put("data", dataArray);

            seriesArray.add(series);
        }

        return seriesArray;
    }

}
