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
package net.tsquery.model;

import net.tsquery.data.hbase.IDMap;

import java.util.Comparator;

public class Tag {

    public ID keyID;
    public ID valueID;
    public String key = "";
    public String value = "";

    private void loadStringFields(IDMap idMap) {
        try {
            key = this.keyID.isNull() ? "NULL" : idMap.getTag(this.keyID);
            value = this.valueID.isNull() ? "NULL" : idMap
                    .getTagValue(this.valueID);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Tag(byte[] rawKeyID, byte[] rawValueID, IDMap idMap) {
        this.keyID = new ID(rawKeyID);
        this.valueID = new ID(rawValueID);
        loadStringFields(idMap);
    }

    public Tag(ID keyID, ID valueID, IDMap idMap) {
        this.keyID = keyID;
        this.valueID = valueID;
        loadStringFields(idMap);
    }

    @Override
    public String toString() {
        return "(" + key + "[" + keyID + "]:" + value + "[" + valueID + "])";
    }

    public static String join(String glue, Tag[] tags) {
        String ret = "";
        for (int i = 0; i < tags.length; i++) {
            if (i > 0) {
                ret += glue;
            }
            ret += tags[i].toString();
        }
        return ret;
    }

    public String toHexString() {
        return keyID.toHexString() + valueID.toHexString();
    }

}
