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

package org.apache.spark.sql.hive

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants._
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.{PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, JobConf, InputFormat => oldInputClass}
import org.apache.hadoop.mapreduce.{InputFormat => newInputClass}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}


private[hive] sealed trait HuldarTableReader {
  def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow]

  def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow]
}

private[hive] class HuldarHadoopTableReader(
  @transient private val attributes: Seq[Attribute],
  @transient private val partitionKeys: Seq[Attribute],
  @transient private val tableDesc: TableDesc,
  @transient private val sparkSession: SparkSession,
  hadoopConf: Configuration)
  extends HuldarTableReader with CastSupport with SQLConfHelper with Logging {

  // Hadoop honors "mapreduce.job.maps" as hint,
  // but will ignore when mapreduce.jobtracker.address is "local".
  // https://hadoop.apache.org/docs/r2.7.6/hadoop-mapreduce-client/hadoop-mapreduce-client-core/
  // mapred-default.xml
  //
  // In order keep consistency with Hive, we will let it be 0 in local mode also.
  private val _minSplitsPerRDD = if (sparkSession.sparkContext.isLocal) {
    0
  } else {
    math.max(hadoopConf.getInt("mapreduce.job.maps", 1),
      sparkSession.sparkContext.defaultMinPartitions)
  }

  SparkHadoopUtil.get.appendS3AndSparkHadoopHiveConfigurations(
    sparkSession.sparkContext.conf, hadoopConf)

  private val _broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

  override def conf: SQLConf = sparkSession.sessionState.conf

  override def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow] =
    makeRDDForTable(
      hiveTable,
      Utils.classForName[Deserializer](tableDesc.getSerdeClassName),
      filterOpt = None)

  private def makeRDDForTable(
    hiveTable: HiveTable,
    deserializerClass: Class[_ <: Deserializer],
    filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
        "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    val localTableDesc = tableDesc
    val broadcastedHadoopConf = _broadcastedHadoopConf

    val tablePath = hiveTable.getPath
    val inputPathStr = applyFilterIfNeeded(tablePath, filterOpt)

    val hadoopRDD = createHadoopRDD(localTableDesc, inputPathStr)

    val attrsWithIndex = attributes.zipWithIndex
    val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.getConstructor().newInstance()
      HuldarDeserializerLock.synchronized {
        deserializer.initialize(hconf, localTableDesc.getProperties)
      }
      HuldarHadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow, deserializer)
    }
    deserializedHadoopRDD
  }

  private def applyFilterIfNeeded(path: Path, filterOpt: Option[PathFilter]): String = {
    filterOpt match {
      case Some(filter) =>
        val fs = path.getFileSystem(hadoopConf)
        val filteredFiles = fs.listStatus(path, filter).map(_.getPath.toString)
        filteredFiles.mkString(",")
      case None => path.toString
    }
  }

  /**
   * 如果实现了新的 org.apache.hadoop.mapreduce.InputFormat 则为真
   * （HiveHBaseTableInputFormat 除外，其中虽然新接口由基 HBase 类实现，
   * 但 Hive 层中的表初始化仅通过旧接口方法发生 - 有关更多详细信息，请参见 SPARK- 32380
   */
  private def compatibleWithNewHadoopRDD(inputClass: Class[_ <: oldInputClass[_, _]]): Boolean =
    classOf[newInputClass[_, _]].isAssignableFrom(inputClass) &&
      !inputClass.getName.equalsIgnoreCase("org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat")

  private def createHadoopRDD(localTableDesc: TableDesc, inputPathStr: String): RDD[Writable] = {
    val inputFormatClazz = localTableDesc.getInputFileFormatClass
    if (compatibleWithNewHadoopRDD(inputFormatClazz)) {
      createNewHadoopRDD(localTableDesc, inputPathStr)
    } else {
      createOldHadoopRDD(localTableDesc, inputPathStr)
    }
  }

  private def createNewHadoopRDD(tableDesc: TableDesc, path: String): RDD[Writable] = {
    val newJobConf = new JobConf(hadoopConf)
    HuldarHadoopTableReader.initializeLocalJobConfFunc(path, tableDesc)(newJobConf)
    val inputFormatClass = tableDesc.getInputFileFormatClass
      .asInstanceOf[Class[newInputClass[Writable, Writable]]]
    createNewHadoopRDD(inputFormatClass, newJobConf)
  }

  private def createOldHadoopRDD(tableDesc: TableDesc, path: String): RDD[Writable] = {
    val initializeJobConfFunc = HuldarHadoopTableReader.initializeLocalJobConfFunc(path, tableDesc) _
    val inputFormatClass = tableDesc.getInputFileFormatClass
      .asInstanceOf[Class[oldInputClass[Writable, Writable]]]
    createOldHadoopRDD(inputFormatClass, initializeJobConfFunc)
  }
  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow] = {
    val partitionToDeserializer = partitions.map(part =>
      (part, part.getDeserializer.getClass.asInstanceOf[Class[Deserializer]])).toMap
    makeRDDForPartitionedTable(partitionToDeserializer, filterOpt = None)
  }

  private def makeRDDForPartitionedTable(
    partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]],
    filterOpt: None.type): RDD[InternalRow] = {
    def verifyPartitionPath(
      partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]]):
    Map[HivePartition, Class[_ <: Deserializer]] = {
      if (!conf.verifyPartitionPath) {
        partitionToDeserializer
      } else {
        val existPathSet = collection.mutable.Set[String]()
        val pathPatternSet = collection.mutable.Set[String]()
        partitionToDeserializer.filter {
          case (partition, parDeserializer) =>
            def updateExistPathSetByPathPattern(pathPatternStr: String): Unit = {
              val pathPattern = new Path(pathPatternStr)
              val fs = pathPattern.getFileSystem(hadoopConf)
              val matches = fs.globStatus(pathPattern)
              matches.foreach(fileStatus => existPathSet += fileStatus.getPath.toString)
            }

            // convert  /demo/data/year/month/day  to  /demo/data/*/*/*/
            def getPathPatternByPath(parNum: Int, tempPath: Path): String = {
              var path = tempPath
              for (i <- (1 to parNum)) path = path.getParent
              val tails = (1 to parNum).map(_ => "*").mkString("/", "/", "/")
              path.toString + tails
            }

            val partPath = partition.getDataLocation
            val partNum = Utilities.getPartitionDesc(partition).getPartSpec.size()
            val pathPatternStr = getPathPatternByPath(partNum, partPath)
            if (!pathPatternSet.contains(pathPatternStr)) {
              pathPatternSet += pathPatternStr
              updateExistPathSetByPathPattern(pathPatternStr)
            }
            existPathSet.contains(partPath.toString)
        }
      }
    }

    val hivePartitionRDDs = verifyPartitionPath(partitionToDeserializer)
      .map { case (partition, partDeserializer) =>
        val partDesc = Utilities.getPartitionDescFromTableDesc(tableDesc, partition, true)
        val partPath = partition.getDataLocation
        val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)

        val partSpec = partDesc.getPartSpec
        val partProps = partDesc.getProperties

        val partColsDelimited: String = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
        val partCols = partColsDelimited.trim().split("/").toSeq

        val partValues = if (partSpec == null) {
          Array.fill(partCols.size)(new String)
        } else {
          partCols.map(col => new String(partSpec.get(col))).toArray
        }

        val broadcastedHiveConf = _broadcastedHadoopConf
        val localDeserializer = partDeserializer

        val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

        val (partitionKeyAttrs, nonPartitionKeyAttrs) =
          attributes.zipWithIndex.partition { case (attr, _) =>
            partitionKeys.contains(attr)
          }

        def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow): Unit = {
          partitionKeyAttrs.foreach { case (attr, ordinal) =>
            val partOrdinal = partitionKeys.indexOf(attr)
            row(ordinal) = cast(Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
          }
        }

        fillPartitionKeys(partValues, mutableRow)

        val tableProperties = tableDesc.getProperties
        val avroSchemaProperties = Seq(AvroTableProperties.SCHEMA_LITERAL,
          AvroTableProperties.SCHEMA_URL).map(_.getPropName)

        val localTableDesc = tableDesc

        createHadoopRDD(partDesc, inputPathStr).mapPartitions { iter =>
          val hconf = broadcastedHiveConf.value.value
          val deserializer = localDeserializer.getConstructor().newInstance()
          //SPARK-13709：对于像 AvroSerDe 这样的 SerDes，一些基本信息（例如 Avro 模式信息）可以在表属性中定义。
          // 这里我们应该在初始化反序列化器之前合并表属性和分区属性。
          // 请注意，分区属性在这里具有更高的优先级，除了 Avro 表属性以支持模式演变：在这种情况下，将使用在表级别给出的属性（有关详细信息，请查看 SPARK-26836）。
          // 例如，分区可能具有与表属性中定义的不同的 SerDe。
          val props = new Properties(tableProperties)
          partProps.asScala.filterNot { case (k, _) =>
            avroSchemaProperties.contains(k) && tableProperties.contains(k)
          }.foreach {
            case (key, value) => props.put(key, value)
          }
          HuldarDeserializerLock.synchronized {
            deserializer.initialize(hconf, props)
          }

          val tableSerDe = localTableDesc.getDeserializerClass.getConstructor().newInstance()
          HuldarDeserializerLock.synchronized {
            tableSerDe.initialize(hconf, tableProperties)
          }
          HuldarHadoopTableReader.fillObject(iter, deserializer, nonPartitionKeyAttrs,
            mutableRow, tableSerDe)
        }
      }.toSeq

    if (hivePartitionRDDs.isEmpty) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
    } else {
      new UnionRDD(hivePartitionRDDs.head.context, hivePartitionRDDs)
    }
  }

  private def createHadoopRDD(partitionDesc: PartitionDesc, inputPathStr: String): RDD[Writable] = {
    val inputFormatClazz = partitionDesc.getInputFileFormatClass
    if (compatibleWithNewHadoopRDD(inputFormatClazz)) {
      createNewHadoopRDD(partitionDesc, inputPathStr)
    } else {
      createOldHadoopRDD(partitionDesc, inputPathStr)
    }
  }

  private def createNewHadoopRDD(partDesc: PartitionDesc, path: String): RDD[Writable] = {
    val newJobConf = new JobConf(hadoopConf)
    HuldarHadoopTableReader.initializeLocalJobConfFunc(path, partDesc.getTableDesc)(newJobConf)
    val inputFormatClass = partDesc.getInputFileFormatClass
      .asInstanceOf[Class[newInputClass[Writable, Writable]]]
    createNewHadoopRDD(inputFormatClass, newJobConf)
  }

  private def createOldHadoopRDD(partDesc: PartitionDesc, path: String): RDD[Writable] = {
    val initializeJobConfFunc =
      HuldarHadoopTableReader.initializeLocalJobConfFunc(path, partDesc.getTableDesc) _
    val inputFormatClass = partDesc.getInputFileFormatClass
      .asInstanceOf[Class[oldInputClass[Writable, Writable]]]
    createOldHadoopRDD(inputFormatClass, initializeJobConfFunc)
  }


  private def createNewHadoopRDD(
    inputFormatClass: Class[newInputClass[Writable, Writable]],
    jobConf: JobConf): RDD[Writable] = {
    val rdd = new NewHadoopRDD(
      sparkSession.sparkContext,
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      jobConf)
    rdd.map(_._2)
  }

  private def createOldHadoopRDD(
    inputClass: Class[oldInputClass[Writable, Writable]],
    initializeJobConfFunc: JobConf => Unit): RDD[Writable] = {
    val rdd = new HadoopRDD(
      sparkSession.sparkContext,
      _broadcastedHadoopConf,
      Some(initializeJobConfFunc),
      inputClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    rdd.map(_._2)
  }

}

private[hive] object HuldarHiveTableUtil {
  def configureJobPropertiesForStorageHandler(
    tableDesc: TableDesc, conf: Configuration, input: Boolean): Unit = {
    val property = tableDesc.getProperties.getProperty(META_TABLE_STORAGE)
    val storageHandler =
      org.apache.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
    }
  }
}

private[hive] object HuldarDeserializerLock

object HuldarHadoopTableReader extends HiveInspectors with Logging {
  def fillObject(
    iterator: Iterator[Writable],
    rawDeser: Deserializer,
    noPartitionKeyAttrs: Seq[(Attribute, Int)],
    mutableRow: SpecificInternalRow,
    tableDeser: Deserializer): Iterator[InternalRow] = {

    // 反序列化,将Hadoop的Writable 转换成 java的对象
    // raw输入和输出
    val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
      rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        rawDeser.getObjectInspector,
        tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
    }
    logDebug(soi.toString)

    val (fieldRefs, fieldOrdinals) = noPartitionKeyAttrs.map { case (attr, ordinal) =>
      soi.getStructFieldRef(attr.name) -> ordinal
    }.toArray.unzip


    // 根据对象检查器类型提前构建特定的解包器，以避免每行的模式匹配和分支成本。
    val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
      _.getFieldObjectInspector match {
        case oi: BooleanObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
        case oi: ByteObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
        case oi: ShortObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
        case oi: IntObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
        case oi: LongObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
        case oi: FloatObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
        case oi: DoubleObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
        case oi: HiveVarcharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveCharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveDecimalObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, HiveShim.toCatalystDecimal(oi, value))
        case oi: TimestampObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(oi.getPrimitiveJavaObject(value)))
        case oi: DateObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setInt(ordinal, DateTimeUtils.fromJavaDate(oi.getPrimitiveJavaObject(value)))
        case oi: BinaryObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, oi.getPrimitiveJavaObject(value))
        case oi =>
          val unwrapper = unwrapperFor(oi)
          (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
      }
    }

    val converter = ObjectInspectorConverters.getConverter(rawDeser.getObjectInspector, soi)

    iterator.map { value =>
      val raw = converter.convert(rawDeser.deserialize(value))
      var i = 0
      val length = fieldRefs.length
      while (i < length) {
        try {
          val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
          if (fieldValue == null) {
            mutableRow.setNullAt(fieldOrdinals(i))
          } else {
            unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
          }
          i += 1
        } catch {
          case ex: Throwable =>
            logError(s"Exception thrown in field <${fieldRefs(i).getFieldName}>")
            throw ex
        }
      }
      mutableRow: InternalRow
    }

  }


  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc)(jobConf: JobConf): Unit = {
    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
    if (tableDesc != null) {
      HuldarHiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, input = true)
      Utilities.copyTablePropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
  }

}


