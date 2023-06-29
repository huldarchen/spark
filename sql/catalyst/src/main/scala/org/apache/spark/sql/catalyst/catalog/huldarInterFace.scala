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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.catalyst.{HuldarFunctionIdentifier, InternalRow}
import org.apache.spark.sql.errors.{QueryCompilationErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.util.Date
import scala.collection.mutable


/**
 * A function defined in the catalog.
 *
 * @param identifier name of the function
 * @param className  fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 * @param resources  resource types and Uris used by the function
 */
case class HuldarCatalogFunction(
  identifier: HuldarFunctionIdentifier,
  className: String,
  resources: Seq[FunctionResource])

/**
 * Storage format, used to describe how a partition or a table is stored.
 */
case class HuldarCatalogStorageFormat(
  locationUri: Option[URI],
  inputFormat: Option[String],
  outputFormat: Option[String],
  serde: Option[String],
  compressed: Boolean,
  properties: Map[String, String]) {

  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()
    locationUri.foreach(l => map.put("Location", l.toString))
    serde.foreach(map.put("Serde Library", _))
    inputFormat.foreach(map.put("InputFormat", _))
    outputFormat.foreach(map.put("OutputFormat", _))
    if (compressed) map.put("Compressed", "")
    SQLConf.get.redactOptions(properties) match {
      case props if props.isEmpty =>
      case props =>
        map.put("Storage Properties", props.map(p => s"${p._1}=${p._2}").mkString("[", ",", "]"))
    }
    map
  }

  override def toString: String = {
    toLinkedHashMap.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("Storage(", ", ", ")")
  }
}

object HuldarCatalogStorageFormat {
  val empty = HuldarCatalogStorageFormat(locationUri = None, inputFormat = None,
    outputFormat = None, serde = None, compressed = false, properties = Map.empty)
}


/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param spec           partition spec values indexed by column name
 * @param storage        storage format of the partition
 * @param parameters     some parameters for the partition
 * @param createTime     creation time of the partition, in milliseconds
 * @param lastAccessTime last access time, in milliseconds
 * @param stats          optional statistics (number of rows, total size, etc.)
 */
case class HuldarCatalogTablePartition(
  spec: HuldarCatalogTypes.TablePartitionSpec,
  storage: HuldarCatalogStorageFormat,
  parameters: Map[String, String] = Map.empty,
  createTime: Long = System.currentTimeMillis(),
  lastAccessTime: Long = -1,
  stats: Option[HuldarCatalogStatistics] = None) {
  def toLinkedHashMap: mutable.LinkedHashMap[String, String] = {
    val map = new mutable.LinkedHashMap[String, String]()

    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(",")
    map.put("Partition Values", s"[$specString]")
    map ++ storage.toLinkedHashMap
    if (parameters.nonEmpty) {
      map.put("Partition Parameters",
        s"{${SQLConf.get.redactOptions(parameters).map(p => s"${p._1}=${p._2}").mkString(", ")}}")
    }
    map.put("Create Time", new Date(createTime).toString)
    val lastAccess = {
      if (lastAccessTime <= 0) "UNKNOWN" else new Date(lastAccessTime).toString
    }
    map.put("Last Access", lastAccess)
    stats.foreach(s => map.put("Partition Statistics", s.simpleString))
    map
  }

  override def toString: String = {
    toLinkedHashMap.map { case (key, value) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("HuldarCatalogTablePartition(\n\t","\n\t", ")")
  }

  def simpleString: String = {
    toLinkedHashMap.map { case (key, value) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", System.lineSeparator(), "")
  }

  def location: URI = storage.locationUri.getOrElse {
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    throw QueryCompilationErrors.partitionNotSpecifyLocationUriError(specString)
  }

  def toRow(partitionSchema: StructType, defaultTimeZondId: String): InternalRow = {
    val caseInsensitiveProperties = CaseInsensitiveMap(storage.properties)
    val timeZoneId = caseInsensitiveProperties.getOrElse(
      DateTimeUtils.TIMEZONE_OPTION, defaultTimeZondId)
    InternalRow.fromSeq(partitionSchema.map { field =>
      val partValue = if (spec(field.name) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
        null
      } else {
        spec(field.name)
      }
      Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
    })
  }

}


case class HuldarCatalogStatistics() {
  def simpleString: String = ???

}

object HuldarCatalogTypes {
  type TablePartitionSpec = Map[String, String]

  lazy val emptyTablePartitionSpec: TablePartitionSpec = Map.empty[String, String]
}
