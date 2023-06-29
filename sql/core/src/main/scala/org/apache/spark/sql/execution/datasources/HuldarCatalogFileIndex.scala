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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

import java.net.URI

class HuldarCatalogFileIndex(
  sparkSession: SparkSession,
  val table: CatalogTable,
  override val sizeInBytes: Long) extends FileIndex {

  protected val hadoopConf: Configuration = sparkSession.sessionState.newHadoopConf()

  assert(table.identifier.database.isDefined,
    "The table identifier must be qualified in CatalogFileIndex")

  private val baseLocation: Option[URI] = table.storage.locationUri



  override def rootPaths: Seq[Path] = ???

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = ???

  override def inputFiles: Array[String] = ???

  override def refresh(): Unit = ???

  override def partitionSchema: StructType = ???
}
