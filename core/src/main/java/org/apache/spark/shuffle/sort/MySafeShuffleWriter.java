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

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.*;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.internal.config.package$;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2022/11/11 16:08
 */
public class MySafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {


    private static final Logger logger = LoggerFactory.getLogger(MySafeShuffleWriter.class);

    private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

    @VisibleForTesting
    static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;
    private final BlockManager blockManager;
    private final TaskMemoryManager memoryManager;
    private final SerializerInstance serializer;
    private final Partitioner partitioner;
    private final ShuffleWriteMetricsReporter writeMetrics;
    private final ShuffleExecutorComponents shuffleExecutorComponents;
    private final int shuffleId;
    private final long mapId;
    private final TaskContext taskContext;
    private final SparkConf sparkConf;
    private final boolean transferToEnabled;
    private final int initialSortBufferSize;
    private final int inputBufferSizeInBytes;


    @Nullable
    private MapStatus mapStatus;
    @Nullable private ShuffleExternalSorter sorter;
    @Nullable private long[] partitionLengths;
    private long peakMemoryUsedBytes = 0;

    /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
    private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
        MyByteArrayOutputStream(int size) { super(size); }
        public byte[] getBuf() { return buf; }
    }

    private MySafeShuffleWriter.MyByteArrayOutputStream serBuffer;

    private SerializationStream serOutputStream;


    private boolean stopping = false;


    public MySafeShuffleWriter(
            BlockManager blockManager,
            TaskMemoryManager memoryManager,
            SerializedShuffleHandle<K, V> handle,
            long mapId,
            TaskContext taskContext,
            SparkConf sparkConf,
            ShuffleWriteMetricsReporter writeMetrics,
            ShuffleExecutorComponents shuffleExecutorComponents) throws SparkException {
        final int numPartitions = handle.dependency().partitioner().numPartitions();
        if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
            throw new IllegalArgumentException(
                    "UnsafeShuffleWriter can only be used for shuffles with at most " +
                            SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
                            " reduce partitions");
        }
        this.blockManager = blockManager;
        this.memoryManager = memoryManager;
        this.mapId = mapId;
        final ShuffleDependency<K, V, V> dep = handle.dependency();
        this.shuffleId = dep.shuffleId();
        this.serializer = dep.serializer().newInstance();
        this.partitioner = dep.partitioner();
        this.writeMetrics = writeMetrics;
        this.shuffleExecutorComponents = shuffleExecutorComponents;
        this.taskContext = taskContext;
        this.sparkConf = sparkConf;
        this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
        this.initialSortBufferSize =
                (int) (long)sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
        this.inputBufferSizeInBytes =
                (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;

        open();
    }

    private void open() throws SparkException{
        assert (sorter == null);
        sorter = new ShuffleExternalSorter(
                memoryManager,
                blockManager,
                taskContext,
                initialSortBufferSize,
                partitioner.numPartitions(),
                sparkConf,
                writeMetrics);
        serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
        serOutputStream = serializer.serializeStream(serBuffer);
    }


    @Override
    public void write(Iterator<Product2<K, V>> records) throws IOException {
        boolean success = false;
        try {
            while (records.hasNext()) {
                insertRecordIntoSorter(records.next());
            }
            closeAndWriteOutput();
            success = true;
        } finally {
            if (sorter != null) {
                try {
                    sorter.cleanupResources();
                } catch (Exception e) {
                    if (success) {
                        throw e;
                    } else {
                        logger.error("In addition to a failure during writing, we failed during" +
                                "cleanup.", e);
                    }
                }
            }
        }
    }
    @VisibleForTesting
    void closeAndWriteOutput() throws IOException {
        assert (sorter != null);
        updatePeakMemoryUsed();
        serBuffer = null;
        serOutputStream = null;
        final SpillInfo[] spills = sorter.closeAndGetSpills();
        try {
            partitionLengths = mergeSpills(spills);
        } finally {
            sorter = null;
            for (SpillInfo spill : spills) {
                if (spill.file.exists() && !spill.file.delete()) {
                    logger.error("Error while deleting spill file {}", spill.file.getPath());
                }
            }
        }
        MapStatus$.MODULE$.apply(
                blockManager.shuffleServerId(), partitionLengths, mapId);
    }

    private long[] mergeSpills(SpillInfo[] spills) throws IOException {
        long[] partitionLengths;
        if (spills.length == 0) {
            final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
                    .createMapOutputWriter(shuffleId, mapId, partitioner.numPartitions());
            return mapWriter.commitAllPartitions(
                    ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE).getPartitionLengths();
        } else if (spills.length == 1) {
            Optional<SingleSpillShuffleMapOutputWriter> maybeSingleFileWriter =
                    shuffleExecutorComponents.createSingleFileMapOutputWriter(shuffleId, mapId);
            if (maybeSingleFileWriter.isPresent()) {
                partitionLengths = spills[0].partitionLengths;
                logger.debug("Merge shuffle spills for mapId {} with length {}", mapId,
                        partitionLengths.length);
                maybeSingleFileWriter.get()
                        .transferMapSpillFile(spills[0].file, partitionLengths, sorter.getChecksums());
            } else {
                partitionLengths = mergeSpillsUsingStandarWriter(spills);
            }
        } else {
            partitionLengths = mergeSpillsUsingStandarWriter(spills);
        }
        return partitionLengths;
    }

    private long[] mergeSpillsUsingStandarWriter(SpillInfo[] spills) {
        // TODO
        return new long[0];
    }

    private void updatePeakMemoryUsed() {
        if (sorter != null) {
            long mem = sorter.getPeakMemoryUsedBytes();
            if (mem > peakMemoryUsedBytes) {
                peakMemoryUsedBytes = mem;
            }
        }
    }

    private void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
        assert (sorter != null);
        final K key = record._1();
        final int partitionId = partitioner.getPartition(key);
        serBuffer.reset();
        serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
        serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
        serOutputStream.flush();

        final int serializedRecordSize = serBuffer.size();
        assert (serializedRecordSize > 0);
        sorter.insertRecord(
                serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        return null;
    }

    @Override
    public long[] getPartitionLengths() {
        return new long[0];
    }
}
