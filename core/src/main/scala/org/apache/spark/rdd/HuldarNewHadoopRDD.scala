// /*
//  *
//  *  * Licensed to the Apache Software Foundation (ASF) under one or more
//  *  * contributor license agreements.  See the NOTICE file distributed with
//  *  * this work for additional information regarding copyright ownership.
//  *  * The ASF licenses this file to You under the Apache License, Version 2.0
//  *  * (the "License"); you may not use this file except in compliance with
//  *  * the License.  You may obtain a copy of the License at
//  *  *
//  *  *    http://www.apache.org/licenses/LICENSE-2.0
//  *  *
//  *  * Unless required by applicable law or agreed to in writing, software
//  *  * distributed under the License is distributed on an "AS IS" BASIS,
//  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  *  * See the License for the specific language governing permissions and
//  *  * limitations under the License.
//  *
//  */
//
// package org.apache.spark.rdd
//
// import java.text.SimpleDateFormat
// import java.util.{Date, Locale}
// import scala.collection.JavaConverters.asScalaBufferConverter
// import scala.reflect.ClassTag
// import org.apache.hadoop.conf.{Configurable, Configuration}
// import org.apache.hadoop.io.Writable
// import org.apache.hadoop.io.compress.CompressionCodecFactory
// import org.apache.hadoop.mapred
// import org.apache.hadoop.mapred.JobConf
// import org.apache.hadoop.mapreduce._
// import org.apache.hadoop.mapreduce.lib.db.DBInputFormat
// import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, DelegatingInputFormat, FileInputFormat, FileSplit, InvalidInputException}
// import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit
// import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
// import org.apache.spark
// import org.apache.spark.deploy.SparkHadoopUtil
// import org.apache.spark.errors.SparkCoreErrors
// import org.apache.spark.executor.InputMetrics
// import org.apache.spark.internal.Logging
// import org.apache.spark.internal.config.{HADOOP_RDD_IGNORE_EMPTY_SPLITS, IGNORE_CORRUPT_FILES, IGNORE_MISSING_FILES, IO_WARNING_LARGEFILETHRESHOLD}
// import org.apache.spark.storage.StorageLevel
// import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager, Utils}
// import org.apache.spark.{InterruptibleIterator, Partition, SerializableWritable, SparkContext, TaskContext}
//
// import java.io.{FileNotFoundException, IOException}
//
//
//
// private[spark] class HuldarNewHadoopPartition(
//   rddId: Int,
//   val index: Int,
//   rawSplit: InputSplit with Writable)
//   extends Partition {
//   val serializableHadoopSplit = new SerializableWritable(rawSplit)
//
//   override def hashCode(): Int = 31 * (31 + rddId) + index
//
//   override def equals(other: Any): Boolean = super.equals(other)
// }
//
// class HuldarNewHadoopRDD[K, V](
//     sc: SparkContext,
//     inputFormatClass: Class[_ <: InputFormat[K, V]],
//     keyClass: Class[K],
//     valueClass: Class[V],
//     @transient private val _conf: Configuration)
//   extends RDD[(K, V)](sc, Nil) with Logging {
//
//   // 创建Hadoop的map任务
//   // 1. 获取配置信息, 这个调用是在executor端进行
//
//   private val confBroadcast = sc.broadcast(new SerializableConfiguration(_conf))
//
//   // 2. Hadoop的任务ID
//   private val jobTrackerId: String = {
//     val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
//     formatter.format(new Date())
//   }
//
//   @transient protected val jobId = new JobID(jobTrackerId, id)
//
//   private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)
//
//   private val ignoreCorruptFiles = sparkContext.conf.get(IGNORE_CORRUPT_FILES)
//
//   private val ignoreMissingFiles = sparkContext.conf.get(IGNORE_MISSING_FILES)
//
//   private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)
//
//
//   def getConf: Configuration = {
//     val conf = confBroadcast.value.value
//     if (shouldCloneJobConf) {
//       HuldarNewHadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
//         if (conf.isInstanceOf[JobConf]) {
//           new JobConf(conf)
//         } else {
//           new Configuration(conf)
//         }
//       }
//     } else {
//       conf
//     }
//   }
//
//
//   override protected def getPartitions: Array[Partition] = {
//     val inputFormat = inputFormatClass.getConstructor().newInstance()
//     _conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
//       Runtime.getRuntime.availableProcessors().toString)
//
//     inputFormat match {
//       case configurable: Configurable =>
//         configurable.setConf(_conf)
//       case _ =>
//     }
//
//     try {
//       val allRowSplits = inputFormat.getSplits(new JobContextImpl(_conf, jobId)).asScala
//       val rawSplit = if (ignoreEmptySplits) {
//         allRowSplits.filter(_.getLength > 0)
//       } else {
//         allRowSplits
//       }
//
//       if (rawSplit.length == 1 && rawSplit.head.isInstanceOf[FileSplit]) {
//         val fileSplit = rawSplit.head.asInstanceOf[FileSplit]
//         val path = fileSplit.getPath
//         if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
//           val codecFactory = new CompressionCodecFactory(_conf)
//           if (Utils.isFileSplittable(path, codecFactory)) {
//             logWarning(s"Loading one large file ${path.toString} with only one partition, " +
//               s"we can increase partition numbers for improving performance.")
//           } else {
//             logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
//               s"partition, because the file is compressed by unsplittable compression codec.")
//           }
//         }
//       }
//
//       val rest = new Array[Partition](rawSplit.size)
//       for (i <- rawSplit.indices) {
//         rest(i) =
//           new HuldarNewHadoopPartition(id, i, rawSplit(i).asInstanceOf[InputSplit with Writable])
//       }
//       rest
//     } catch {
//       case e: InvalidInputException if ignoreMissingFiles =>
//         logWarning(s"${_conf.get(FileInputFormat.INPUT_DIR)} doesn't exist and no" +
//           s" partitions returned from this path.", e)
//         Array.empty[Partition]
//     }
//
//   }
//
//   override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
//     val iter = new Iterator[(K, V)] {
//
//       private val split = theSplit.asInstanceOf[HuldarNewHadoopPartition]
//       logInfo(s"Input slit: ${split.serializableHadoopSplit}")
//       private val conf = getConf
//
//       private val inputMetrics = context.taskMetrics().inputMetrics
//       private val existingBytesRead: Long = inputMetrics.bytesRead
//
//       split.serializableHadoopSplit.value match {
//         case fs: FileSplit =>
//           InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
//         case _ =>
//           InputFileBlockHolder.unset()
//       }
//
//       private val getBytesReadCallback: Option[() => Long] =
//         split.serializableHadoopSplit.value match {
//           case _: FileSplit | _: CombineFileSplit =>
//             Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
//           case _ => None
//         }
//
//       private def updateBytesRead(): Unit = {
//         getBytesReadCallback.foreach { getbytesRead =>
//           inputMetrics.setBytesRead(existingBytesRead + getbytesRead)
//         }
//       }
//
//
//       private val format = inputFormatClass.getConstructor().newInstance()
//       format match {
//         case configurable: Configurable =>
//           configurable.setConf(conf)
//         case _ =>
//       }
//       private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
//       private val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
//
//       private var finished = false
//       private var reader =
//         try {
//           val _reader = format.createRecordReader(
//             split.serializableHadoopSplit.value, hadoopAttemptContext)
//           _reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
//           _reader
//         } catch {
//           case e: FileNotFoundException if ignoreMissingFiles =>
//             logWarning(s"Skipped missing file: ${split.serializableHadoopSplit}", e)
//             finished = true
//             null
//           // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
//           case e: FileNotFoundException if !ignoreMissingFiles => throw e
//           case e: IOException if ignoreCorruptFiles =>
//             logWarning(
//               s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
//               e)
//             finished = true
//             null
//         }
//
//       context.addTaskCompletionListener[Unit] { _ =>
//         updateBytesRead()
//         close
//       }
//
//       private var havePair = false
//
//       override def hasNext: Boolean = {
//         if (!finished && !havePair) { // 没有结束 并且还有
//           try {
//             // 是否结束
//             finished = !reader.nextKeyValue
//           } catch {
//             case e: FileNotFoundException if ignoreMissingFiles =>
//               logWarning(s"Skipped missing file: ${split.serializableHadoopSplit}", e)
//               finished = true
//             // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
//             case e: FileNotFoundException if !ignoreMissingFiles => throw e
//             case e: IOException if ignoreCorruptFiles =>
//               logWarning(
//                 s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
//                 e)
//               finished = true
//           }
//           if (finished) {
//             close()
//           }
//           havePair = !finished
//         }
//         !finished
//       }
//
//       override def next(): (K, V) = {
//         if (!hasNext) {
//           throw SparkCoreErrors.endOfStreamError()
//         }
//         havePair = false
//         if (!finished) {
//           inputMetrics.incRecordsRead(1)
//         }
//         if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
//           updateBytesRead()
//         }
//         (reader.getCurrentKey, reader.getCurrentValue)
//       }
//
//       private def close(): Unit = {
//         if (reader != null) {
//           InputFileBlockHolder.unset()
//         }
//         try {
//           reader.close()
//         } catch {
//           case e: Exception =>
//             if (!(ShutdownHookManager.inShutdown())) {
//               logWarning("Exception in RecordReader.close()", e)
//             }
//         } finally {
//           reader = null
//         }
//         if (getBytesReadCallback.isDefined) {
//           updateBytesRead()
//         } else if (split.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
//                    split.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
//           try {
//             inputMetrics.incBytesRead(split.serializableHadoopSplit.value.getLength)
//           } catch {
//             case e: IOException =>
//               logWarning("Unable to get input size to set InputMetrics for task", e)
//           }
//         }
//       }
//     }
//
//     new InterruptibleIterator[(K, V)](context, iter)
//   }
//
//
//
// }
//
// private[spark] object HuldarNewHadoopRDD {
//   /**
//    * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
//    * Therefore, we synchronize on this lock before calling new Configuration().
//    */
//   val CONFIGURATION_INSTANTIATION_LOCK = new Object()
//
//   private[spark] class HuldarNewHadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
//     prev: RDD[T],
//     f: (InputSplit, Iterator[T]) => Iterator[U],
//     preservesPartitioning: Boolean = false)
//   extends RDD[U](prev) {
//
//     override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None
//
//     override def compute(split: Partition, context: TaskContext): Iterator[U] = {
//       val partition = split.asInstanceOf[HuldarNewHadoopPartition]
//       val inputSplit = partition.serializableHadoopSplit.value
//       f(inputSplit, firstParent[T].iterator(split, context))
//     }
//
//     override protected def getPartitions: Array[Partition] = firstParent[T].partitions
//   }
// }
