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
package com.facebook.tsdb.tsdash.server.model;

import java.util.Arrays;

import com.facebook.tsdb.tsdash.server.data.hbase.IDMap;

public class TagsArray {

    public static final ID[] NATURAL_ORDER = new ID[0];

    private final IDMap idMap;

    private final Tag[] tags;
    private int[] order;
    private ID[] priTags = NATURAL_ORDER;

    public TagsArray(Tag[] tags, ID[] priTags, IDMap idMap) {
        this.tags = tags;
        this.priTags = priTags;
        this.idMap = idMap;
        createOrderSet(priTags);
    }

    private void createOrderSet(ID[] tagsPri) {
        boolean[] isPri = new boolean[tags.length];
        Arrays.fill(isPri, false);
        order = new int[tags.length];
        int head = 0; // this is the head of the order stack
        for (ID priTagKey : tagsPri) {
            int priTagPos = Arrays.binarySearch(tags, new Tag(priTagKey,
                    priTagKey, idMap), Tag.keyComparator());
            if (priTagPos < 0) {
                System.err.println("Tag ID " + priTagKey + " not found in this"
                        + " tag list");
                continue;
            }
            isPri[priTagPos] = true;
            order[head] = priTagPos;
            head++;
        }
        // the unspecified tags are ordered by natural ordering
        for (int i = 0; i < tags.length; i++) {
            if (!isPri[i]) {
                order[head] = i;
                head++;
            }
        }
    }

    public TagsArray copy() {
        Tag[] newTags = Arrays.copyOf(tags, tags.length);
        return new TagsArray(newTags, priTags, idMap);
    }

    public int binarySearch(Tag tag) {
        return Arrays.binarySearch(tags, tag, Tag.keyComparator());
    }

    public Tag get(int index) {
        return tags[index];
    }

    public Tag getOrdered(int index) {
        return tags[order[index]];
    }

    public int length() {
        return tags.length;
    }

    @Override
    public String toString() {
        String ret = "Natural order: ";
        for (Tag tag : tags) {
            ret += tag + " ";
        }
        ret += "\nUser given order:";
        for (int anOrder : order) {
            ret += tags[anOrder] + " ";
        }
        return ret;
    }

    public Tag[] asArray() {
        return tags;
    }

}
