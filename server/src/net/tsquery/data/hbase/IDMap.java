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
package net.tsquery.data.hbase;

import java.io.IOException;

import net.tsquery.model.ID;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

public class IDMap {

    private final IDMapSyncLoader syncMetricsLoader = new IDMapSyncLoader(
            HBaseDataProvider.METRIC_QUALIFIER.getBytes());
    private final IDMapSyncLoader syncTagsLoader = new IDMapSyncLoader(
            HBaseDataProvider.TAG_QUALIFIER.getBytes());
    private final IDMapSyncLoader syncTagValuesLoader = new IDMapSyncLoader(
            HBaseDataProvider.TAG_VALUE_QUALIFIER.getBytes());

    private static String getEqual(ImmutableBiMap<String, ID> map, ID id) {
        for (ID valueID : map.inverse().keySet()) {
            if (valueID.compareTo(id) == 0) {
                return map.inverse().get(valueID);
            }
        }
        return null;
    }

    // metrics

    public String[] getMetrics() throws IOException {
        ImmutableBiMap<String, ID> metricsMap = syncMetricsLoader.get();
        ImmutableSet<String> strings = metricsMap.keySet();
        return strings.toArray(new String[strings.size()]);
    }

    public ID getMetricID(String metric) throws IOException,
            IDNotFoundException {
        ImmutableBiMap<String, ID> metricsMap = syncMetricsLoader.get();
        ID metricID = metricsMap.get(metric);
        if (metricID == null) {
            throw new IDNotFoundException("metric '" + metric + "' not found");
        }
        return metricID;
    }

    // tags

    public String[] getTags() throws IOException {
        ImmutableBiMap<String, ID> tagsMap = syncTagsLoader.get();
        ImmutableSet<String> strings = tagsMap.keySet();
        return strings.toArray(new String[strings.size()]);
    }

    public ID getTagID(String tag) throws IOException, IDNotFoundException {
        ImmutableBiMap<String, ID> tagsMap = syncTagsLoader.get();
        ID tagID = tagsMap.get(tag);
        if (tagID == null) {
            throw new IDNotFoundException("tag '" + tag + "' not found");
        }
        return tagID;
    }

    public String getTag(ID tagID) throws IOException, IDNotFoundException {
        ImmutableBiMap<String, ID> tagsMap = syncTagsLoader.get();
        String tag = getEqual(tagsMap, tagID);
        if (tag == null) {
            throw new IDNotFoundException("tag id '" + tagID + "' not found");
        }
        return tag;
    }

    // tag values

    public String[] getTagValues() throws IOException {
        ImmutableBiMap<String, ID> tagValuesMap = syncTagValuesLoader.get();
        ImmutableSet<String> strings = tagValuesMap.keySet();
        return strings.toArray(new String[strings.size()]);
    }

    public ID getTagValueID(String tagValue) throws IOException,
            IDNotFoundException {
        ImmutableBiMap<String, ID> tagValuesMap = syncTagValuesLoader.get();
        ID tagValueID = tagValuesMap.get(tagValue);
        if (tagValueID == null) {
            throw new IDNotFoundException("tag value '" + tagValue
                    + "' not found");
        }
        return tagValueID;
    }

    public String getTagValue(ID valueID) throws IOException,
            IDNotFoundException {
        ImmutableBiMap<String, ID> tagValuesMap = syncTagValuesLoader.get();
        String tagValue = getEqual(tagValuesMap, valueID);
        if (tagValue == null) {
            throw new IDNotFoundException("tag value id '" + valueID
                    + "' not found");
        }
        return tagValue;
    }
}
