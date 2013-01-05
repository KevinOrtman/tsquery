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
 * added downsample parameter
 *
 * @author kevin ortman
 *
 */
package net.tsquery.model;

import org.json.simple.JSONObject;
import java.util.HashMap;

public class MetricQuery {

    public String name;
    public HashMap<String, String> tags = null;
    public String aggregator = null;
    public int downsample = 0;
    public boolean rate = false;

    public static HashMap<String, String> decodeTags(JSONObject tagsObj) {
        HashMap<String, String> tags = new HashMap<String, String>();
        for (Object tagKeyObj : tagsObj.keySet()) {
            tags.put((String) tagKeyObj, (String) tagsObj.get(tagKeyObj));
        }
        return tags;
    }

    public static MetricQuery fromJSONObject(JSONObject src) {
        MetricQuery newQuery = new MetricQuery();

        newQuery.name = (String) src.get("name");
        if(newQuery.name == null || newQuery.name.length() == 0)
            throw new IllegalArgumentException("Required parameter 'name' not specified for all metrics");

        if (src.get("rate") != null) {
            newQuery.rate = (Boolean) src.get("rate");
        }

        newQuery.tags = new HashMap<String, String>();
        if(src.get("tags") != null)
        newQuery.tags = decodeTags((JSONObject) src.get("tags"));

        newQuery.aggregator = "sum";
        if(src.get("aggregator") != null)
            newQuery.aggregator = ((String)src.get("aggregator")).toLowerCase();

        if(src.get("downsample") != null)
            newQuery.downsample = tryParse(src.get("downsample"), 0);

        return newQuery;
    }

    @Override
    public String toString() {
        String ret = "Name: " + name + '\n';
        ret += "tags: " + tags + '\n';
        ret += "aggregator: " + aggregator + '\n';
        ret += "downsample: " + downsample + '\n';
        ret += "rate: " + rate + '\n';
        ret += '\n';
        return ret;
    }

    private static int tryParse(Object value, int def) {
        int v = def;
        try {
            v = Integer.parseInt((String)value);
        }
        catch (Exception e) {
        }
        return v;
    }
}
