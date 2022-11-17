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

package org.apache.spark.shuffle.sort;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.util.collection.unsafe.sort.RadixSort;

import java.util.Comparator;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2022/11/14 14:48
 */
public class HuldarShuffleInMemorySorter {

    private static final class SortCompartor implements Comparator<PackedRecordPointer> {

        @Override
        public int compare(PackedRecordPointer left, PackedRecordPointer right) {
            return Integer.compare(left.getPartitionId(), right.getPartitionId());
        }
    }

    private static final SortCompartor SORT_COMPARTOR = new SortCompartor();

    private final MemoryConsumer consumer;

    private LongArray array;

    private final boolean useRadixSort;

    private int pos = 0;

    private int usableCapacity = 0;

    private final int initialSize;

    HuldarShuffleInMemorySorter(MemoryConsumer consumer, int initialSize, boolean useRadixSort) {
        this.consumer = consumer;
        assert (initialSize > 0);
        this.initialSize = initialSize;
        this.useRadixSort = useRadixSort;
        this.array = consumer.allocateArray(initialSize);
        this.usableCapacity = getUsableCapacity();
    }

    private int getUsableCapacity() {
        return (int) (array.size() /  (useRadixSort ? 2 : 1.5));
    }

    public void free() {
        if (array != null) {
            consumer.freeArray(array);
            array = null;
        }
    }


    public int numRecords() {
        return pos;
    }


    public void reset() {
        pos = 0;
        if (consumer != null) {
            consumer.freeArray(array);

            array = null;
            usableCapacity = 0;
            array = consumer.allocateArray(initialSize);
            usableCapacity = getUsableCapacity();
        }
    }

    public void expandPointerArray(LongArray newArray) {
        assert (newArray.size() > array.size());
        Platform.copyMemory(
                array.getBaseObject(),
                array.getBaseOffset(),
                newArray.getBaseObject(),
                newArray.getBaseOffset(),
                pos * 8L
        );
        consumer.freeArray(array);
        array = newArray;
        usableCapacity = getUsableCapacity();
    }

    public boolean hasSpaceForAnotherRecord() {
        return pos < usableCapacity;
    }

    public long getMemoryUsage() {
        return array.size() * 8;
    }

    public void insertRecord(long recordPointer, int partitionId) {
        if (!hasSpaceForAnotherRecord()) {
            throw new IllegalStateException("There is no space for new record");
        }
        array.set(pos, PackedRecordPointer.packPointer(recordPointer, partitionId));
        pos++;
    }

    public static final class ShuffleSorterIterator {
        private final LongArray pointerArray;
        private final int limit;
        final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
        private int position = 0;

        ShuffleSorterIterator(int numRecords, LongArray pointerArray, int startingPosition) {
            this.limit = numRecords + startingPosition;
            this.pointerArray = pointerArray;
            this.position = startingPosition;
        }

        public boolean hasNext() {
            return position < limit;
        }

        public void loadNext() {
            packedRecordPointer.set(pointerArray.get(position));
            position++;
        }
    }

    public ShuffleSorterIterator getSortedIterator() {
        int offset = 0;
        if (useRadixSort) {
            offset = RadixSort.sort(
                    array, pos,
                    PackedRecordPointer.PARTITION_ID_START_BYTE_INDEX,
                    PackedRecordPointer.PARTITION_ID_END_BYTE_INDEX, false, false);
        } else {
            MemoryBlock unused = new MemoryBlock(
                    array.getBaseObject(),
                    array.getBaseOffset() + pos * 8L,
                    (array.size() - pos) * 8L);
            LongArray buffer = new LongArray(unused);
            Sorter<PackedRecordPointer, LongArray> sorter =
                    new Sorter<>(new ShuffleSortDataFormat(buffer));

            sorter.sort(array, 0, pos, SORT_COMPARTOR);
        }
        return new ShuffleSorterIterator(pos, array, offset);
    }

}
