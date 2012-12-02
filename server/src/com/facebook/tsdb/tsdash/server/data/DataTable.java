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
package com.facebook.tsdb.tsdash.server.data;

import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.facebook.tsdb.tsdash.server.model.DataPoint;
import com.facebook.tsdb.tsdash.server.model.Metric;
import com.facebook.tsdb.tsdash.server.model.TagsArray;

public class DataTable {

    private final Metric[] metrics;

    public DataTable(Metric[] metrics) {
        this.metrics = metrics;
    }

    @SuppressWarnings("unchecked")
    public JSONArray toJSONObject() {
        JSONArray seriesArray = new JSONArray();

        for (Metric metric : metrics) {
            for (TagsArray t : metric.timeSeries.keySet()) {
                JSONObject series = new JSONObject();
                series.put("name", renderLineTitle(metric, t));
                series.put("data", generateTimeSeriesData(metric, t));
                seriesArray.add(series);
            }
        }

        return seriesArray;
    }

    private String renderLineTitle(Metric metric, TagsArray tags) {
        String suffix = metric.isRate() ? " /s" : "";
        return metric.getName() + ": " + tags.getTitle() + suffix;
    }

    public JSONArray generateTimeSeriesData(Metric metric, TagsArray timeSeriesKey)
    {
        JSONArray arrary = new JSONArray();

        for(DataPoint point : metric.timeSeries.get(timeSeriesKey))
        {
            JSONArray values = new JSONArray();
            values.add(point.ts * 1000);
            values.add(point.value);
            arrary.add(values);
        }
        return arrary;
    }
}
