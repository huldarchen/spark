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

import com.google.common.util.concurrent.Striped
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.HuldarHiveMetastoreCatalog.mergeWithMetastoreSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode._
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.util.Locale
import scala.util.control.NonFatal

private[hive] class HuldarHiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {

  private def sessionSate = sparkSession.sessionState

  // HiveSessionCatalog 这个是
  private def catalogProxy = sparkSession.sessionState.catalog

  private val tableCreateLocks = Striped.lazyWeakLock(100)

  private def withTableCreateLock[A](tableName: QualifiedTableName, f: => A): A = {
    val lock = tableCreateLocks.get(tableName)
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private def isOrcProperty(key: String) =
    key.startsWith("orc.") || key.contains(".orc.")

  private def isParquetProperty(key: String) =
    key.startsWith("parquet.") || key.contains(".parquet.")


  private[hive] def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    val key = QualifiedTableName(
      table.database.getOrElse(sessionSate.catalog.getCurrentDatabase).toLowerCase,
      table.table.toLowerCase)
    catalogProxy.getCachedTable(key)
  }

  private def getCached(
    tableIdentifier: QualifiedTableName,
    pathsInMetastore: Seq[Path],
    schemaInMetastore: StructType,
    expectedFileFormat: Class[_ <: FileFormat],
    partitionSchema: Option[StructType]): Option[LogicalRelation] = {
    catalogProxy.getCachedTable(tableIdentifier) match {
      case null => None
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass
        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            val useCached =
              relation.location.rootPaths.toSet == pathsInMetastore.toSet &&
                DataTypeUtils.sameType(logical.schema, schemaInMetastore) &&
                relation.bucketSpec.isEmpty &&
                relation.partitionSchema == partitionSchema.getOrElse(StructType(Nil))
            if (useCached) {
              Some(logical)
            } else {
              catalogProxy.invalidateCachedTable(tableIdentifier)
              None
            }
          case _ =>
            logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
              s"However, we are getting a ${relation.fileFormat} from the metastore cache. " +
              "This cached entry will be invalidated.")
            catalogProxy.invalidateCachedTable(tableIdentifier)
            None
        }
      case other =>
        logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
          s"However, we are getting a $other from the metastore cache. " +
          "This cached entry will be invalidated.")
        catalogProxy.invalidateCachedTable(tableIdentifier)
        None
    }
  }


  def convert(relation: HiveTableRelation, isWrite: Boolean): LogicalRelation = {
    // 存储类型
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)

    if (serde.contains("parquet")) {
      val options = relation.tableMeta.properties.filterKeys(isParquetProperty).toMap ++
        relation.tableMeta.storage.properties +
        (ParquetOptions.MERGE_SCHEMA -> SQLConf.get.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING).toString)
      convertToLogicalRelation(relation, options, classOf[ParquetFileFormat], "parquet", isWrite)
    } else {
      val options = relation.tableMeta.properties.filterKeys(isOrcProperty).toMap ++
        relation.tableMeta.storage.properties
      if (SQLConf.get.getConf(SQLConf.ORC_IMPLEMENTATION) == "native") {
        convertToLogicalRelation(
          relation,
          options,
          classOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat],
          "orc",
          isWrite)
      } else {
        convertToLogicalRelation(
          relation,
          options,
          classOf[org.apache.spark.sql.hive.orc.OrcFileFormat],
          "orc",
          isWrite)
      }
    }
  }

  def convertStorageFormat(storage: CatalogStorageFormat): CatalogStorageFormat = {
    val serde = storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    if (serde.contains("parquet")) {
      val options = storage.properties + (ParquetOptions.MERGE_SCHEMA ->
        SQLConf.get.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING).toString)
      storage.copy(
        serde = None,
        properties = options)
    } else {
      val options = storage.properties
      if (SQLConf.get.getConf(SQLConf.ORC_IMPLEMENTATION) == "native") {
        storage.copy(
          serde = None,
          properties = options
        )
      } else {
        storage.copy(
          serde = None,
          properties = options
        )
      }
    }
  }

  private def convertToLogicalRelation(
    relation: HiveTableRelation,
    options: Map[String, String],
    fileFormatClass: Class[_ <: FileFormat],
    fileType: String,
    isWrite: Boolean): LogicalRelation = {
    val metastoreSchema = relation.tableMeta.schema
    val tableIdentifier =
      QualifiedTableName(relation.tableMeta.database, relation.tableMeta.identifier.table)

    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val tablePath = new Path(relation.tableMeta.location)
    val fileFormat = fileFormatClass.getConstructor().newInstance()
    val bucketSpec = relation.tableMeta.bucketSpec
    val (hiveOptions, hiveBucketSpec) =
      if (isWrite) {
        (options.updated(BucketingUtils.optionForHiveCompatibleBucketWrite, "true"),
          bucketSpec)
      } else {
        (options, None)
      }

    val result = if (relation.isPartitioned) {
      val partitionSchema = relation.tableMeta.partitionSchema
      val rootPaths = if (lazyPruningEnabled) {
        Seq(tablePath)
      } else {
        // By convention (for example, see CatalogFileIndex), the definition of a
        // partitioned table's paths depends on whether that table has any actual partitions.
        // Partitioned tables without partitions use the location of the table's base path.
        // Partitioned tables with partitions use the locations of those partitions' data
        // locations,_omitting_ the table's base path.
        val paths = sparkSession.sharedState.externalCatalog
          .listPartitions(tableIdentifier.database, tableIdentifier.name)
          .map(p => new Path(p.storage.locationUri.get))
        if (paths.isEmpty) {
          Seq(tablePath)
        } else {
          paths
        }
      }

      withTableCreateLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          rootPaths,
          metastoreSchema,
          fileFormatClass,
          Some(partitionSchema))

        val logicalRelation = cached.getOrElse {
          val sizeInBytes = relation.stats.sizeInBytes.toLong
          val fileIndex = {
            val index = new CatalogFileIndex(sparkSession, relation.tableMeta, sizeInBytes)
            if (lazyPruningEnabled) {
              index
            } else {
              index.filterPartitions(Nil)
            }
          }

          val updatedTable = inferIfNeeded(relation, hiveOptions, fileFormat, Option(fileIndex))

          val enableDynamicPartition = hiveOptions.updated(DataSourceUtils.PARTITION_OVERWRITE_MODE,
            PartitionOverwriteMode.DYNAMIC.toString)
          val fsRelation = HadoopFsRelation(
            location = fileIndex,
            partitionSchema = partitionSchema,
            dataSchema = updatedTable.dataSchema,
            bucketSpec = hiveBucketSpec,
            fileFormat = fileFormat,
            options = enableDynamicPartition)(sparkSession = sparkSession)
          val created = LogicalRelation(fsRelation, updatedTable)
          catalogProxy.cacheTable(tableIdentifier, created)
          created
        }
        logicalRelation
      })
    } else {
      val rootPath = tablePath
      withTableCreateLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          Seq(rootPath),
          metastoreSchema,
          fileFormatClass,
          None)
        val logicalRelation = cached.getOrElse {
          val updateTable = inferIfNeeded(relation, hiveOptions, fileFormat)
          val created =
            LogicalRelation(
              DataSource(
                sparkSession = sparkSession,
                paths = rootPath.toString :: Nil,
                userSpecifiedSchema = Option(updateTable.dataSchema),
                bucketSpec = hiveBucketSpec,
                options = hiveOptions.filter { case (k, _) => !k.equalsIgnoreCase("path") },
                className = fileType).resolveRelation(),
              table = updateTable)
          catalogProxy.cacheTable(tableIdentifier, created)
          created
        }
        logicalRelation
      })
    }
    if (result.output.length != relation.output.length) {
      throw new AnalysisException(
        s"Converted table has ${result.output.length} columns, " +
          s"but source Hive table has ${relation.output.length} columns. " +
          s"Set ${HiveUtils.CONVERT_METASTORE_PARQUET.key} to false, " +
          s"or recreate table ${relation.tableMeta.identifier} to workaround.")
    }
    if (result.output.zip(relation.output).forall {
      case (a1, a2) => a1.dataType == a2.dataType
    }) {
      throw new AnalysisException(
        s"Column in converted table has different data type with source Hive table's. " +
          s"Set ${HiveUtils.CONVERT_METASTORE_PARQUET.key} to false, " +
          s"or recreate table ${relation.tableMeta.identifier} to workaround.")
    }
    val newOutput = result.output.zip(relation.output).map {
      case (a1, a2) => a1.withExprId(a2.exprId)
    }
    result.copy(output = newOutput)
  }

  private def inferIfNeeded(
    relation: HiveTableRelation,
    options: Map[String, String],
    fileFormat: FileFormat,
    fileIndexOpt: Option[FileIndex] = None): CatalogTable = {
    val inferenceMode = sparkSession.sessionState.conf.caseSensitiveInferenceMode
    val shouldInfer = (inferenceMode != NEVER_INFER) && !relation.tableMeta.schemaPreservesCase
    val tableName = relation.tableMeta.identifier.unquotedString
    if (shouldInfer) {
      logInfo(s"Inferring case-sensitive schema for table $tableName (inference mode: " +
        s"$inferenceMode)")
      val fileIndex = fileIndexOpt.getOrElse {
        val rootPath = new Path(relation.tableMeta.location)
        new InMemoryFileIndex(sparkSession, Seq(rootPath), options, None)
      }

      val inferredSchema = fileFormat
        .inferSchema(
          sparkSession,
          options,
          fileIndex.listFiles(Nil, Nil).flatMap(_.files))
        .map(mergeWithMetastoreSchema(relation.tableMeta.dataSchema, _))

      inferredSchema match {
        case Some(dataSchema) =>
          if (inferenceMode == INFER_AND_SAVE) {
            updateDataSchema(relation.tableMeta.identifier, dataSchema)
          }
          val newSchema = StructType(dataSchema ++ relation.tableMeta.partitionSchema)
          relation.tableMeta.copy(schema = newSchema)
        case None =>
          logWarning(s"Unable to infer schema for table $tableName from file format " +
            s"$fileFormat (inference mode: $inferenceMode). Using metastore schema.")
          relation.tableMeta
      }
    } else {
      relation.tableMeta
    }
  }

  private def updateDataSchema(identifier: TableIdentifier, newDataSchema: StructType): Unit = try {
    logInfo(s"Saving case-sensitive schema for table ${identifier.unquotedString}")
    sparkSession.sessionState.catalog.alterTableDataSchema(identifier, newDataSchema)
  } catch {
    case NonFatal(ex) =>
      logWarning(s"Unable to save case-sensitive schema for table ${identifier.unquotedString}", ex)
  }

}


private[hive] object HuldarHiveMetastoreCatalog {
  def mergeWithMetastoreSchema(
    metastoreSchema: StructType,
    inferredSchema: StructType): StructType = try {
    val metastoreFields = metastoreSchema.map(f => f.name.toLowerCase -> f).toMap
    val missingNullables = metastoreFields
      .filterKeys(inferredSchema.map(_.name.toLowerCase).contains(_))
      .values
      .filter(_.nullable)
    val inferredFields = StructType(inferredSchema ++ missingNullables)
      .map(f => f.name.toLowerCase -> f).toMap
    StructType(metastoreSchema.map(f => f.copy(name = inferredFields(f.name.toLowerCase).name)))
  } catch {
    case NonFatal(_) =>
      val msg =
        s"""Detected conflicting schemas when merging the schema obtained from the Hive
           | Metastore with the one inferred from the file format. Metastore schema:
           |${metastoreSchema.prettyJson}
           |
           |Inferred schema:
           |${inferredSchema.prettyJson}
       """.stripMargin
      throw new SparkException(msg)
  }
}
