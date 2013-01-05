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

import java.util.Arrays;

import net.tsquery.data.hbase.IDMap;

public class TagsArray {

    public static final ID[] NATURAL_ORDER = new ID[0];

    private final IDMap idMap;

    private final Tag[] tags;
    private ID[] priTags = NATURAL_ORDER;

    public TagsArray(Tag[] tags, ID[] priTags, IDMap idMap) {
        this.tags = tags;
        this.priTags = priTags;
        this.idMap = idMap;
    }

    public TagsArray copy() {
        Tag[] newTags = Arrays.copyOf(tags, tags.length);
        return new TagsArray(newTags, priTags, idMap);
    }

    @Override
    public String toString() {
        String ret = "";
        for (Tag tag : tags) {
            ret += tag + " ";
        }
        return ret;
    }

    public Tag[] asArray() {
        return tags;
    }

}
