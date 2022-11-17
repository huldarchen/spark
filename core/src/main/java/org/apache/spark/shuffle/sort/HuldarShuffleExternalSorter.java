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

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.*;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.internal.config.package$;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.zip.Checksum;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2022/11/12 09:39
 */
public class HuldarShuffleExternalSorter extends MemoryConsumer implements ShuffleChecksumSupport {

    private static final Logger logger = LoggerFactory.getLogger(HuldarShuffleExternalSorter.class);

    static final int DISK_WRITER_BUFFER_SIZE = 1024 * 1024;

    private final int numPartitions;
    private final TaskMemoryManager taskMemoryManager;
    private final BlockManager blockManager;
    private final TaskContext taskContext;
    private final ShuffleWriteMetricsReporter writeMetrics;

    private final int numElementsForSpillThreshold;
    private final int fileBufferSizeBytes;
    private final int diskWriteBufferSize;

    private final LinkedList<MemoryBlock> allocatedPage = new LinkedList<>();
    private final LinkedList<SpillInfo> spills = new LinkedList<>();

    private long peakMemoryUsedBytes;
    @Nullable private ShuffleInMemorySorter inMemSorter;
    @Nullable private MemoryBlock currentPage = null;

    private long pageCursor = -1;
    private final Checksum[] partitionChecksums;


    HuldarShuffleExternalSorter(
            TaskMemoryManager memoryManager,
            BlockManager blockManager,
            TaskContext taskContext,
            int initialSize,
            int numPartitions,
            SparkConf conf,
            ShuffleWriteMetricsReporter writeMetrics) {
        super(memoryManager,
                Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
                memoryManager.getTungstenMemoryMode());
        this.taskMemoryManager = memoryManager;
        this.blockManager = blockManager;
        this.taskContext = taskContext;
        this.numPartitions = numPartitions;
        this.fileBufferSizeBytes =
                (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
        this.numElementsForSpillThreshold =
                (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
        this.writeMetrics = writeMetrics;
        this.inMemSorter = new ShuffleInMemorySorter(
                this, initialSize, (Boolean) conf.get(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT()));
        this.peakMemoryUsedBytes = getMemoryUsage();
        this.diskWriteBufferSize =
                (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
        this.partitionChecksums = createPartitionChecksums(numPartitions, conf);
    }

    public long[] getChecksums() {
        return getChecksumValues(partitionChecksums);
    }


    private long getMemoryUsage() {
        long totalPageSize = 0;
        for (MemoryBlock page : allocatedPage) {
            totalPageSize += page.size();
        }
        return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
            return 0L;
        }
        logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
                Thread.currentThread().getId(),
                Utils.bytesToString(getMemoryUsage()),
                spills.size(),
                spills.size() > 1 ? " times" : " time");
        writeSortedFile(false);

        final long spillSize = freeMemory();
        inMemSorter.reset();
        taskContext.taskMetrics().incDiskBytesSpilled(spillSize);
        return spillSize;
    }

    private long freeMemory() {
        updatePeakMemoryUsed();
        long memoryFreed = 0;
        for (MemoryBlock block : allocatedPage) {
            memoryFreed += block.size();
            freePage(block);
        }
        allocatedPage.clear();
        currentPage = null;
        pageCursor = 0;
        return memoryFreed;
    }

    private void updatePeakMemoryUsed() {
        long mem = getMemoryUsage();
        if (mem > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = mem;
        }
    }

    private void writeSortedFile(boolean isLastFile) {
        final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
                inMemSorter.getSortedIterator();
        if (!sortedRecords.hasNext()) {
            return;
        }

        final ShuffleWriteMetricsReporter writeMetricsToUse;

        if (isLastFile) {
            writeMetricsToUse = writeMetrics;
        } else {
            writeMetricsToUse = new ShuffleWriteMetrics();
        }

        final byte[] writeBuffer = new byte[diskWriteBufferSize];

        final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
                blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = spilledFileInfo._2;
        final TempShuffleBlockId blockId = spilledFileInfo._1;
        final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

        final SerializerInstance ser = DummySerializerInstance.INSTANCE;

        int currentPartition = -1;
        final FileSegment committedSegment;
        try(DiskBlockObjectWriter writer =
                    blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse)) {

            final int uaoSize = UnsafeAlignedOffset.getUaoSize();
            while (sortedRecords.hasNext()) {
                sortedRecords.loadNext();
                final int partition = sortedRecords.packedRecordPointer.getPartitionId();
                assert (partition >= currentPartition);
                if (partition != currentPartition) {
                    if (currentPartition != -1) {
                        final FileSegment fileSegment = writer.commitAndGet();
                        spillInfo.partitionLengths[currentPartition] = fileSegment.length();
                    }
                    currentPartition = partition;
                    if (partitionChecksums.length > 0) {
                        writer.setChecksum(partitionChecksums[currentPartition]);
                    }
                }

                final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
                final Object recordPage = taskMemoryManager.getPage(recordPointer);
                final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
                int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
                long recordReadPosition = recordOffsetInPage + uaoSize;
                while (dataRemaining > 0) {
                    final int toTransfer = Math.min(diskWriteBufferSize, dataRemaining);
                    Platform.copyMemory(
                            recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
                    writer.write(writeBuffer, 0, toTransfer);
                    recordReadPosition += toTransfer;
                    dataRemaining -= toTransfer;
                }
                writer.recordWritten();
            }
            committedSegment = writer.commitAndGet();
        }

        if (currentPartition != -1) {
            spillInfo.partitionLengths[currentPartition] = committedSegment.length();
            spills.add(spillInfo);
        }

        if (!isLastFile) {
            writeMetrics.incRecordsWritten(
                    ((ShuffleWriteMetrics)writeMetricsToUse).recordsWritten());
            taskContext.taskMetrics().incDiskBytesSpilled(
                    ((ShuffleWriteMetrics)writeMetricsToUse).recordsWritten());
        }
    }

    public void cleanupResources() {
        freeMemory();
        if (inMemSorter != null) {
            inMemSorter.free();
            inMemSorter = null;
        }
        for (SpillInfo spill : spills) {
            if (spill.file.exists() && !spill.file.delete()) {
                logger.error("Unable to delete spill file {}", spill.file.getPath());
            }
        }
    }

    public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
        throws IOException {
        assert(inMemSorter != null);
        if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
            logger.info("Spilling data because number of spilledRecords crossed the threshold " +
                    numElementsForSpillThreshold);
            spill();
        }

        growPointerArrayIfNecessary();
        final int uaoSize = UnsafeAlignedOffset.getUaoSize();
        final int required = length + uaoSize;
        acquireNewPageIfNecessary(required);

        assert (currentPage != null);
        final Object base = currentPage.getBaseObject();
        final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
        UnsafeAlignedOffset.putSize(base, pageCursor, length);
        pageCursor += uaoSize;
        Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
        pageCursor += length;
        inMemSorter.insertRecord(recordAddress, partitionId);
    }

    private void acquireNewPageIfNecessary(int required) {
        if (currentPage == null ||
            pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
            currentPage = allocatePage(required);
            pageCursor = currentPage.getBaseOffset();
            allocatedPage.add(currentPage);
        }

    }

    private void growPointerArrayIfNecessary() throws IOException {
        assert(inMemSorter != null);
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
            long used = inMemSorter.getMemoryUsage();
            LongArray array;
            try {
                array = allocateArray(used / 8 * 2);
            } catch (TooLargePageException e) {
                spill();
                return;
            } catch (SparkOutOfMemoryError e) {
                if (!inMemSorter.hasSpaceForAnotherRecord()) {
                    logger.error("Unable to grow the pointer array");
                    throw e;
                }
                return;
            }
            if (inMemSorter.hasSpaceForAnotherRecord()) {
                freeArray(array);
            } else {
                inMemSorter.expandPointerArray(array);
            }
        }

    }

    public SpillInfo[] closeAndGetSpills() throws IOException {
        if (inMemSorter != null) {
            writeSortedFile(true);
            freeMemory();
            inMemSorter.free();
            inMemSorter = null;
        }
        return spills.toArray(new SpillInfo[spills.size()]);
    }


    long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

}
