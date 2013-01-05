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

import net.tsquery.data.TsdbDataProvider;
import net.tsquery.data.TsdbDataProviderFactory;
import net.tsquery.model.Metric;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * fetches the header information (tags set, common tags) for a given time frame
 *
 * @author cgheorghe
 *
 */
public class MetricHeaderEndpoint extends TsdbServlet {

    private static final long serialVersionUID = 1L;

    /**
     * GET params:
     * "metric" - metric name
     * "from" - start of the time range
     * "to" - end of the time range
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        response.setContentType("application/json");

        PrintWriter out = response.getWriter();
        try {
            // decode parameters
            long ts = System.currentTimeMillis();

            // decode parameters
            String jsonParams = request.getParameter("params");
            if (jsonParams == null) {
                throw new IllegalArgumentException("Required parameter 'params' not specified");
            }

            JSONObject jsonParamsObj = (JSONObject) JSONValue.parse(jsonParams);
            if(jsonParamsObj == null) {
                throw new IllegalArgumentException("Required parameter 'params' is not a valid JSON object");
            }

            long tsFrom = this.getRequiredTimeStamp(jsonParamsObj, "tsFrom");
            long tsTo = this.getRequiredTimeStamp(jsonParamsObj, "tsTo");

            String metricName = (String) jsonParamsObj.get("metric");
            if (metricName == null || metricName.length() == 0) {
                throw new IllegalArgumentException("Required parameter 'metric' string not specified or empty");
            }

            TsdbDataProvider dataProvider = TsdbDataProviderFactory.get();
            Metric metric = dataProvider.fetchMetricHeader(metricName, tsFrom, tsTo);

            doSendResponse(request, out, metric.toJSONString());

            long loadTime = System.currentTimeMillis() - ts;
            logger.info("[Header] time frame: " + (tsTo - tsFrom) + "s, "
                    + "metric: " + metricName + ", "
                    + "load time: " + loadTime + "ms");
        } catch (Exception e) {
            out.println(getErrorResponse(e));
        }
        out.close();
    }
}
