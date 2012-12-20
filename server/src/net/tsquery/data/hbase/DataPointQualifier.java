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
package net.tsquery.data.hbase;

public class DataPointQualifier {

    private static final int QUALIFIER_BYTES = 2;
    private static final int QUALIFIER_FLAGS_BITS = 4;

    public static byte[] packedOffset(long offset) {
        byte[] packed = new byte[QUALIFIER_BYTES];
        offset = offset << QUALIFIER_FLAGS_BITS;
        long shift = 0;
        for (int i = QUALIFIER_BYTES - 1; i >= 0; i--) {
            packed[i] = (byte) ((offset >> shift) & 0xFF);
            shift += 8;
        }
        return packed;
    }

}
