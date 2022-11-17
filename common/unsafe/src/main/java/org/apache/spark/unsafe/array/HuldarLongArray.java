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
 *
 */

package org.apache.spark.unsafe.array;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2022/11/15 22:23
 */
public final class HuldarLongArray {

    private static final long WIDTH = 8;

    private final MemoryBlock memory;
    private final Object baseObj;
    private final long baseOffset;
    private final long length;

    public HuldarLongArray(MemoryBlock memory) {
        assert memory.size() < (long) Integer.MAX_VALUE * 8: "Array size >= Integer.MAX_VALUE elements";
        this.memory = memory;
        this.baseObj = memory.getBaseObject();
        this.baseOffset = memory.getBaseOffset();
        this.length = memory.size() / WIDTH;
    }

    public MemoryBlock memoryBlock() {
        return memory;
    }

    public Object getBaseObj() {
        return baseObj;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long size() {
        return length;
    }

    public void zeroOut() {
        for (long off = baseOffset; off < baseOffset + length * WIDTH; off += WIDTH) {
            Platform.putLong(baseObj, off, 0);
        }
    }



    public void set(int index, long value) {
        assert index >= 0 : "index (" + index + ") should < length (" + length + ")";
        assert index < length : "index (" + index + ") should < length (" + length + ")";
         Platform.putLong(baseObj, baseOffset + index * WIDTH, value);
    }

    public long get(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < length : "index (" + index + ") should < length (" + length + ")";
        return Platform.getLong(baseObj, baseOffset + index * WIDTH);
    }
}
