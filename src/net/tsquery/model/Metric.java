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
package net.tsquery.model;

import com.google.common.primitives.UnsignedBytes;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

public class Metric {

    protected static Logger logger = Logger
            .getLogger("com.facebook.tsdb.services");

    private final byte[] id;
    private final String name;
    public TreeMap<TagsArray, ArrayList<DataPoint>> timeSeries = new TreeMap<TagsArray, ArrayList<DataPoint>>();

    public Metric(byte[] id, String name) {
        this.id = id;
        this.name = name;
    }

    private static boolean arrayIsSorted(ArrayList<DataPoint> array) {
        for (int i = 0; i < array.size() - 1; i++) {
            if (array.get(i).compareTo(array.get(i + 1)) > 0) {
                return false;
            }
        }
        return true;
    }

    public HashMap<String, HashSet<String>> getTagsSet() {
        HashMap<String, HashSet<String>> tagsSet =
            new HashMap<String, HashSet<String>>();
        for (TagsArray rowTags : timeSeries.keySet()) {
            for (Tag tag : rowTags.asArray()) {
                if (!tagsSet.containsKey(tag.key)) {
                    tagsSet.put(tag.key, new HashSet<String>());
                }
                HashSet<String> values = tagsSet.get(tag.key);
                if (!tag.valueID.isNull() && !values.contains(tag.value)) {
                    values.add(tag.value);
                }
            }
        }
        return tagsSet;
    }

    public HashSet<String> getCommonTags(Set<String> tagsSet) {
        HashSet<String> commonTags = new HashSet<String>();
        HashMap<String, Integer> tagCount = new HashMap<String, Integer>();
        for (String tag : tagsSet) {
            tagCount.put(tag, 0);
        }
        // count tags
        for (TagsArray rowTags : timeSeries.keySet()) {
            for (Tag tag : rowTags.asArray()) {
                tagCount.put(tag.key, tagCount.get(tag.key) + 1);
            }
        }
        // select only those tags that are in all rows fetched
        for (String tag : tagCount.keySet()) {
            if (tagCount.get(tag) == timeSeries.size()) {
                commonTags.add(tag);
            }
        }
        return commonTags;
    }

    @SuppressWarnings("unchecked")
    private JSONObject encodeTagsSet(HashMap<String, HashSet<String>> tagsSet) {
        JSONObject tagsSetObj = new JSONObject();
        for (String tag : tagsSet.keySet()) {
            JSONArray tagValuesArray = new JSONArray();
            if (tagsSet.get(tag) != null) {
                for (String value : tagsSet.get(tag)) {
                    tagValuesArray.add(value);
                }
            }
            tagsSetObj.put(tag, tagValuesArray);
        }
        return tagsSetObj;
    }

    @SuppressWarnings("unchecked")
    private JSONArray encodeCommonTags(HashSet<String> commonTags) {
        JSONArray commonTagsArray = new JSONArray();
        for (String tag : commonTags) {
            commonTagsArray.add(tag);
        }
        return commonTagsArray;
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        JSONObject topObj = new JSONObject();
        topObj.put("name", name);
        HashMap<String, HashSet<String>> tagsSet = getTagsSet();
        topObj.put("tags", encodeTagsSet(tagsSet));
        topObj.put("commontags", encodeCommonTags(getCommonTags(tagsSet.keySet())));

        int rows = 0;
        for (ArrayList<DataPoint> arrayList : timeSeries.values()) {
            rows += arrayList.size();
        }

        topObj.put("rows", rows);
        return topObj;
    }

    public String toJSONString() {
        return toJSONObject().toJSONString();
    }

    @Override
    public String toString() {
        String ret = "Metric " + UnsignedBytes.join("", id) + '\n';
        for (TagsArray tagsArray : timeSeries.keySet()) {
            ret += Tag.join(" ", tagsArray.asArray()) + "\n";
            ret += "datapoints sorted: "
                    + arrayIsSorted(timeSeries.get(tagsArray));
            ret += "\n";
        }
        return ret;
    }

}
