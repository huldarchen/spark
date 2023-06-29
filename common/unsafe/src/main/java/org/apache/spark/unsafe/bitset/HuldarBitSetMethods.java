/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.spark.unsafe.bitset;

import org.apache.spark.unsafe.HuldarPlatform;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2023/4/23 21:13
 */
public class HuldarBitSetMethods {
    private static final long WORD_SIZE = 8;

    private HuldarBitSetMethods() {

    }


    public static void set(Object baseObject, long baseOffset, int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        final long mask = 1L << (index & 0x3f);
        final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;
        final long word = HuldarPlatform.getLong(baseObject, wordOffset);
        HuldarPlatform.putLong(baseObject, wordOffset, word | mask); // TODO & | 的使用场景是什么?
    }

    public static void main(String[] args) {
        System.out.println(Integer.toBinaryString(0x3f));
        System.out.println(8 & 0x3f);
        System.out.println((1 << (62 & 0x3f)));// ????
    }


}
