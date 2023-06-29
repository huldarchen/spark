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
// package org.apache.spark.sql.hive.client
//
// import java.io.PrintStream
// import java.lang.reflect.InvocationTargetException
// import java.lang.{Iterable => JIterable}
// import java.nio.charset.StandardCharsets.UTF_8
// import java.util
// import java.util.concurrent.TimeUnit.MILLISECONDS
// import java.util.{Locale, Map => JMap}
//
// import scala.collection.JavaConverters._
// import scala.collection.mutable.ArrayBuffer
//
// import org.apache.hadoop.conf.Configuration
// import org.apache.hadoop.hive.common.StatsSetupConst
// import org.apache.hadoop.hive.conf.HiveConf
// import org.apache.hadoop.hive.conf.HiveConf.ConfVars
// import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType => HiveTableType}
// import org.apache.hadoop.hive.metastore.api.{Database => HiveDatabase, Table => MetaStoreApiTable, _}
// import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException, Partition => HivePartition, Table => HiveTable}
// import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC
// import org.apache.hadoop.hive.ql.session.SessionState
// import org.apache.hadoop.hive.serde.serdeConstants
// import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe
// import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
// import org.apache.hadoop.security.UserGroupInformation
// import org.apache.spark.{SparkConf, SparkException}
// import org.apache.spark.internal.Logging
// import org.apache.spark.sql.catalyst.TableIdentifier
// import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, NoSuchDatabaseException, NoSuchPartitionException, NoSuchPartitionsException, PartitionsAlreadyExistException}
// import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
// import org.apache.spark.sql.catalyst.catalog._
// import org.apache.spark.sql.catalyst.expressions.Expression
// import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
// import org.apache.spark.sql.catalyst.util.CharVarcharUtils
// import org.apache.spark.sql.connector.catalog.SupportsNamespaces._
// import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
// import org.apache.spark.sql.hive.HiveExternalCatalog
// import org.apache.spark.sql.hive.HiveExternalCatalog.DATASOURCE_SCHEMA
// import org.apache.spark.sql.internal.SQLConf
// import org.apache.spark.sql.types.{DataType, StructField, StructType}
// import org.apache.spark.util.{CircularBuffer, Utils}
//
// private[hive] class HuldarHiveClientImpl(
//   override val version: HiveVersion,
//   warehouseDir: Option[String],
//   sparkConf: SparkConf,
//   hadoopConf: JIterable[JMap.Entry[String, String]],
//   extraConfig: Map[String, String],
//   initClassLoader: ClassLoader,
//   val clientLoader: IsolatedClientLoader) extends HiveClient with Logging {
//
//   private class RawHiveTableImpl(override val rawTable: HiveTable) extends RawHiveTable {
//     override def toCatalogTable: CatalogTable = convertHiveTableToCatalogTable(rawTable)
//
//   }
//
//   import HuldarHiveClientImpl._
//
//   // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
//   private val outputBuffer = new CircularBuffer()
//
//   private val shim = version match {
//     case hive.v12 => new Shim_v0_12()
//     case hive.v13 => new Shim_v0_13()
//     case hive.v14 => new Shim_v0_14()
//     case hive.v1_0 => new Shim_v1_0()
//     case hive.v1_1 => new Shim_v1_1()
//     case hive.v1_2 => new Shim_v1_2()
//     case hive.v2_0 => new Shim_v2_0()
//     case hive.v2_1 => new Shim_v2_1()
//     case hive.v2_2 => new Shim_v2_2()
//     case hive.v2_3 => new Shim_v2_3()
//     case hive.v3_0 => new Shim_v3_0()
//     case hive.v3_1 => new Shim_v3_1()
//   }
//
//
//   val state: SessionState = {
//     val original = Thread.currentThread().getContextClassLoader
//     if (clientLoader.isolationOn) {
//       Thread.currentThread().setContextClassLoader(initClassLoader)
//       try {
//         newState()
//       } finally {
//         Thread.currentThread().setContextClassLoader(original)
//       }
//     } else {
//       val ret = SessionState.get
//       if (ret != null) {
//         warehouseDir.foreach { dir =>
//           ret.getConf.setVar(ConfVars.METASTOREWAREHOUSE, dir)
//         }
//         ret
//       } else {
//         newState()
//       }
//     }
//   }
//   logInfo(
//     "Warehouse location for Hive client " +
//       s"(Version ${version.fullVersion}) is ${conf.getVar(ConfVars.METASTOREWAREHOUSE)}")
//
//
//   private def newState(): SessionState = {
//     val hiveConf = newHiveConf(sparkConf, hadoopConf, extraConfig, Some(initClassLoader))
//     val state = new SessionState(hiveConf)
//     if (clientLoader.cachedHive != null) {
//       Hive.set(clientLoader.cachedHive.asInstanceOf[Hive])
//     }
//     state.getConf.setClassLoader(clientLoader.classLoader)
//     shim.setCurrentSessionState(state)
//     state.out = new PrintStream(outputBuffer, true, UTF_8.name)
//     state.err = new PrintStream(outputBuffer, true, UTF_8.name())
//     state
//   }
//
//   def conf: HiveConf = {
//     val hiveConf = state.getConf
//
//     val isEmbeddedMetaStore = {
//       val msUri = hiveConf.getVar(ConfVars.METASTOREURIS)
//       val msConnUrl = hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY)
//       (msUri == null || msUri.trim.isEmpty) &&
//         (msConnUrl != null && msConnUrl.startsWith("jdbc:derby"))
//     }
//
//     if (isEmbeddedMetaStore) {
//       hiveConf.setBoolean("hive.metastore.schema.verification", false)
//       hiveConf.setBoolean("datanucleus.schema.autoCreateAll", true)
//     }
//     hiveConf
//   }
//
//   override val userName: String = UserGroupInformation.getCurrentUser.getShortUserName
//
//   override def getConf(key: String, defaultValue: String): String = {
//     conf.get(key, defaultValue)
//   }
//
//   private val retryLimit = conf.getIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES)
//   private val retryDelayMillis = shim.getMetastoreClientConnectRetryDelayMillis(conf)
//
//   private def retryLocked[A](f: => A): A = clientLoader.synchronized {
//     val deadLine = System.nanoTime() + (retryLimit * retryDelayMillis * 1e6).toLong
//     var numTries = 0
//     var caughtException: Exception = null
//
//     do {
//       numTries += 1
//       try {
//         return f
//       } catch {
//         case e: Exception if causedByThrift(e) =>
//           caughtException = e
//           logWarning(
//             "HiveClient got thrift exception, destroying client and retrying " +
//               s"(${retryLimit - numTries}) tries remaining", e)
//           clientLoader.cachedHive = null
//           Thread.sleep(retryDelayMillis)
//       }
//     } while (numTries <= retryLimit && System.nanoTime() < deadLine)
//
//     if (System.nanoTime() > deadLine) {
//       logWarning("Deadline exceeded")
//     }
//     throw caughtException
//   }
//
//   private def causedByThrift(e: Throwable): Boolean = {
//     var target = e
//     // 找到是thrift的错误
//     while (target != null) {
//       val msg = target.getMessage
//       if (msg != null && msg.matches("(?s).*(TApplication|TProtocol|TTransport)Exception.*")) {
//         return true
//       }
//       target = target.getCause
//     }
//     false
//   }
//
//
//   private def client: Hive = {
//     if (clientLoader.cachedHive != null) {
//       clientLoader.cachedHive.asInstanceOf[Hive]
//     } else {
//       val c = getHive(conf)
//       clientLoader.cachedHive = c
//       c
//     }
//   }
//
//   private def msClient: IMetaStoreClient = {
//     shim.getMSC(client)
//   }
//   override def getState: Any = withHiveState(state)
//
//   override def withHiveState[A](f: => A): A = retryLocked {
//     val original = Thread.currentThread().getContextClassLoader
//     val originalConfLoader = state.getConf.getClassLoader
//
//     Thread.currentThread().setContextClassLoader(clientLoader.classLoader)
//     state.getConf.setClassLoader(clientLoader.classLoader)
//
//     Hive.set(client)
//     getHive(conf)
//     shim.setCurrentSessionState(state)
//     val ret = try {
//       f
//     } catch {
//       case e: NoClassDefFoundError if e.getMessage.contains("apache/hadoop/hive/serde2/SerDe") =>
//         throw QueryExecutionErrors.serDeInterfaceNotFoundError(e)
//     } finally {
//       state.getConf.setClassLoader(originalConfLoader)
//       Thread.currentThread().setContextClassLoader(original)
//     }
//     ret
//   }
//
//   override def setOut(stream: PrintStream): Unit = {
//     state.out = stream
//   }
//
//   override def setInfo(stream: PrintStream): Unit = {
//     state.info = stream
//   }
//
//   override def setError(stream: PrintStream): Unit = {
//     state.err = stream
//   }
//
//   private def setCurrentDatabaseRaw(db: String): Unit = {
//     if (state.getCurrentDatabase != db) {
//       if (databaseExists(db)) {
//         state.setCurrentDatabase(db)
//       } else {
//         throw NoSuchDatabaseException(db)
//       }
//     }
//   }
//
//   override def setCurrentDatabase(databaseName: String): Unit = {
//     setCurrentDatabaseRaw(databaseName)
//   }
//
//
//   override def createDatabase(
//     database: CatalogDatabase,
//     ignoreIfExists: Boolean): Unit = withHiveState {
//     val hiveDb = toHiveDatabase(database, Some(userName))
//     try {
//       shim.createDatabase(client, hiveDb, ignoreIfExists)
//     } catch {
//       case _: AlreadyExistsException =>
//         throw new DatabaseAlreadyExistsException(database.name)
//     }
//   }
//
//   override def dropDatabase(
//     name: String,
//     ignoreIfNotExists: Boolean,
//     cascade: Boolean): Unit = withHiveState {
//     try {
//       shim.dropDatabase(client, name, true, ignoreIfNotExists, cascade)
//     } catch {
//       case e: HiveException if e.getMessage.contains(s"Database $name is not empty") =>
//         throw QueryCompilationErrors.cannotDropNonemptyDatabaseError(name)
//     }
//   }
//
//   override def alterDatabase(database: CatalogDatabase): Unit = withHiveState {
//     if (!getDatabase(database.name).locationUri.equals(database.locationUri)) {
//       if (!(version.equals(hive.v3_0) || version.equals(hive.v3_1))) {
//         throw QueryCompilationErrors.alterDatabaseLocationUnsupportedError(version.fullVersion)
//       }
//     }
//     val hiveDb = toHiveDatabase(database)
//     shim.alterDatabase(client, database.name, hiveDb)
//   }
//
//   private def toHiveDatabase(
//     database: CatalogDatabase, userName: Option[String] = None): HiveDatabase = {
//     val props = database.properties
//     val hiveDb = new HiveDatabase(
//       database.name,
//       database.description,
//       CatalogUtils.URIToString(database.locationUri),
//       (props -- Seq(PROP_OWNER)).asJava)
//     props.get(PROP_OWNER).orElse(userName).foreach { ownerName =>
//       shim.setDatabaseOwnerName(hiveDb, ownerName)
//     }
//     hiveDb
//   }
//
//
//   override def getDatabase(dbName: String): CatalogDatabase = withHiveState {
//     Option(shim.getDatabase(client, dbName)).map { d =>
//       val params = Option(d.getParameters).map(_.asScala.toMap).getOrElse(Map()) ++
//         Map(PROP_OWNER -> shim.getDatabaseOwnerName(d))
//       CatalogDatabase(
//         name = d.getName,
//         description = Option(d.getDescription).getOrElse(""),
//         locationUri = CatalogUtils.stringToURI(d.getLocationUri),
//         properties = params)
//     }.getOrElse(throw NoSuchDatabaseException(dbName))
//   }
//
//
//   override def databaseExists(dbName: String): Boolean = withHiveState {
//     shim.databaseExists(client, dbName)
//   }
//
//
//   override def listDatabases(pattern: String): Seq[String] = withHiveState {
//     shim.getDatabasesByPattern(client, pattern)
//   }
//
//
//   private def getRawTableOption(dbName: String, tableName: String): Option[HiveTable] = {
//     Option(shim.getTable(client, dbName, tableName, false))
//   }
//
//   private def getRawTablesByName(dbName: String, tableNames: Seq[String]): Seq[HiveTable] = {
//     try {
//       shim.recordHiveCall()
//       msClient.getTableObjectsByName(dbName, tableNames.asJava).asScala
//         .map(extraFixesForNonView).map(new HiveTable(_)).toSeq
//     } catch {
//       case ex: Exception =>
//         throw QueryExecutionErrors.cannotFetchTablesOfDatabaseError(dbName, ex)
//     }
//   }
//
//   override def tableExists(dbName: String, tableName: String): Boolean = withHiveState {
//     getRawTableOption(dbName, tableName).nonEmpty
//   }
//
//
//   override def getTablesByName(
//     dbName: String,
//     tableNames: Seq[String]): Seq[CatalogTable] = withHiveState {
//     getRawTablesByName(dbName, tableNames).map(convertHiveTableToCatalogTable)
//   }
//
//
//   override def getTableOption(
//     dbName: String,
//     tableName: String): Option[CatalogTable] = withHiveState {
//     logDebug(s"Looking up $dbName.$tableName")
//     getRawTableOption(dbName, tableName).map(convertHiveTableToCatalogTable)
//   }
//
//
//   override def getRawHiveTableOption(
//     dbName: String,
//     tableName: String): Option[RawHiveTable] = withHiveState {
//     logDebug(s"Looking up $dbName.$tableName")
//     getRawTableOption(dbName, tableName).map(new RawHiveTableImpl(_))
//   }
//
//   private def convertHiveTableToCatalogTable(h: HiveTable): CatalogTable = {
//     val (cols, partCols) = try {
//       (h.getCols.asScala.map(fromHiveColumn), h.getPartCols.asScala.map(fromHiveColumn))
//     } catch {
//       case ex: SparkException =>
//         throw QueryExecutionErrors.convertHiveTableToCatalogTableError(
//           ex, h.getDbName, h.getTableName)
//     }
//     val schema = StructType((cols ++ partCols).toSeq)
//     val bucketSpec = if (h.getNumBuckets > 0) {
//       val sortColumnOrders = h.getSortCols.asScala
//
//       val allAscendingSorted = sortColumnOrders.forall(_.getOrder == HIVE_COLUMN_ORDER_ASC)
//
//       val sortColumnNames = if (allAscendingSorted) {
//         sortColumnOrders.map(_.getCol)
//       } else {
//         Seq.empty
//       }
//       Option(BucketSpec(h.getNumBuckets, h.getBucketCols.asScala.toSeq, sortColumnNames.toSeq))
//     } else {
//       None
//     }
//
//     val unsupportedFeatures = ArrayBuffer.empty[String]
//     if (!h.getSkewedColNames.isEmpty) {
//       unsupportedFeatures += "skewed columns"
//     }
//
//     if (h.getStorageHandler != null) {
//       unsupportedFeatures += "storage handler"
//     }
//
//     if (h.getTableType == HiveTableType.VIRTUAL_VIEW && partCols.nonEmpty) {
//       unsupportedFeatures += "partitioned view"
//     }
//
//     val properties = Option(h.getParameters).map(_.asScala.toMap).orNull
//
//     val ignoredProperties = scala.collection.mutable.Map.empty[String, String]
//     for(key <- HiveStatisticsProperties; value <- properties.get(key)) {
//       ignoredProperties += key -> value
//     }
//
//     val excludedTableProperties = HiveStatisticsProperties ++ Set(
//       // The property value of "comment" is moved to the dedicated field "comment"
//       "comment",
//       // For EXTERNAL_TABLE, the table properties has a particular field "EXTERNAL". This is added
//       // in the function toHiveTable.
//       "EXTERNAL"
//     )
//     val filteredProperties = properties.filterNot {
//       case (key, _) => excludedTableProperties.contains(key)
//     }
//
//     val comment = properties.get("comment")
//
//     CatalogTable(
//       identifier = TableIdentifier(h.getTableName, Option(h.getDbName)),
//       tableType = h.getTableType match {
//         case HiveTableType.EXTERNAL_TABLE => CatalogTableType.EXTERNAL
//         case HiveTableType.MANAGED_TABLE => CatalogTableType.MANAGED
//         case HiveTableType.VIRTUAL_VIEW => CatalogTableType.VIEW
//         case unsupportedType =>
//           val tableTypeStr = unsupportedType.toString.toLowerCase(Locale.ROOT).replace("_", " ")
//           throw QueryCompilationErrors.hiveTableTypeUnsupportedError(tableTypeStr)
//       },
//       schema = schema,
//       partitionColumnNames = partCols.map(_.name).toSeq,
//       bucketSpec = bucketSpec,
//       owner = Option(h.getOwner).getOrElse(""),
//       createTime = h.getTTable.getCreateTime.toLong * 1000,
//       lastAccessTime = h.getLastAccessTime.toLong * 1000,
//       storage = CatalogStorageFormat(
//         locationUri = shim.getDataLocation(h).map(CatalogUtils.stringToURI),
//         inputFormat = Option(h.getTTable.getSd.getInputFormat).orElse {
//           Option(h.getStorageHandler).map(_.getInputFormatClass.getName)
//         },
//         outputFormat = Option(h.getTTable.getSd.getOutputFormat).orElse {
//           Option(h.getStorageHandler).map(_.getOutputFormatClass.getName)
//         },
//         compressed = h.getTTable.getSd.isCompressed,
//         serde = Option(h.getSerializationLib),
//         properties = Option(h.getTTable.getSd.getSerdeInfo.getParameters)
//           .map(_.asScala.toMap).orNull
//       ),
//       properties = filteredProperties,
//       stats = readHiveStats(properties),
//       comment = comment,
//       viewOriginalText = Option(h.getViewOriginalText),
//       viewText = Option(h.getViewExpandedText),
//       unsupportedFeatures = unsupportedFeatures.toSeq,
//       ignoredProperties = ignoredProperties.toMap)
//   }
//
//   override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = withHiveState {
//     verifyColumnDataType(table.dataSchema)
//     shim.createTable(client, toHiveTable(table, Some(userName)), ignoreIfExists)
//   }
//
//   override def dropTable(
//     dbName: String,
//     tableName: String,
//     ignoreIfNotExists: Boolean,
//     purge: Boolean): Unit = withHiveState {
//     shim.dropTable(client, dbName, tableName, true, ignoreIfNotExists, purge)
//   }
//
//   override def alterTable(
//     dbName: String,
//     tableName: String,
//     table: CatalogTable): Unit = withHiveState {
//     verifyColumnDataType(table.schema)
//     val hiveTable = toHiveTable(
//       table.copy(properties = table.ignoredProperties ++ table.properties), Option(userName))
//     val qualifiedTableName = s"$dbName.$tableName"
//     shim.alterTable(client, qualifiedTableName, hiveTable)
//   }
//
//   override def alterTableDataSchema(
//     dbName: String,
//     tableName: String,
//     newDataSchema: StructType,
//     schemaProps: Map[String, String]): Unit = withHiveState {
//     val oldTable = shim.getTable(client, dbName, tableName)
//     verifyColumnDataType(newDataSchema)
//     val hiveCols = newDataSchema.map(toHiveColumn)
//     oldTable.setFields(hiveCols.asJava)
//
//     val it = oldTable.getParameters.entrySet.iterator
//     while (it.hasNext) {
//       val entry = it.next()
//       if (CatalogTable.isLargeTableProp(DATASOURCE_SCHEMA, entry.getKey)) {
//         it.remove()
//       }
//     }
//
//     schemaProps.foreach { case (k, v) => oldTable.setProperty(k, v) }
//
//     val qualifiedTableName = s"$dbName.$tableName"
//     shim.alterTable(client, qualifiedTableName, oldTable)
//   }
//
//   override def createPartitions(
//     db: String,
//     table: String,
//     parts: Seq[CatalogTablePartition
//     ], ignoreIfExists: Boolean): Unit = withHiveState {
//     def replaceExistException(e: Throwable): Unit = e match {
//       case _: HiveException if e.getCause.isInstanceOf[AlreadyExistsException] =>
//         throw new PartitionsAlreadyExistException(db, table, parts.map(_.spec))
//     }
//     try {
//       shim.createPartitions(client, db, table, parts, ignoreIfExists)
//     } catch {
//       case e: InvocationTargetException => replaceExistException(e.getCause)
//       case e: Throwable => replaceExistException(e)
//     }
//   }
//
//   override def dropPartitions(
//     db: String,
//     table: String,
//     specs: Seq[TablePartitionSpec],
//     ignoreIfNotExists: Boolean,
//     purge: Boolean,
//     retainData: Boolean): Unit = withHiveState {
//     val hiveTable = shim.getTable(client, db, table, true /* throw exception */)
//     val matchingParts = specs.flatMap { s =>
//       assert(s.values.forall(_.nonEmpty), s"partition spec '$s' is invalid")
//       val parts = shim.getPartitions(client, hiveTable, s.asJava)
//       if (parts.isEmpty && !ignoreIfNotExists) {
//         throw new NoSuchPartitionsException(db, table, specs)
//       }
//       parts.map(_.getValues)
//     }.distinct
//
//     val droppedParts = ArrayBuffer.empty[util.List[String]]
//
//     matchingParts.foreach { partition =>
//       try {
//         shim.dropPartition(client, db, table, partition, !retainData, purge)
//       } catch {
//         case e: Exception =>
//           val remainingParts = matchingParts.toBuffer --= droppedParts
//           logError(
//             s"""
//                |======================
//                |Attempt to drop the partition specs in table '$table' database '$db':
//                |${specs.mkString("\n")}
//                |In this attempt, the following partitions have been dropped successfully:
//                |${droppedParts.mkString("\n")}
//                |The remaining partitions have not been dropped:
//                |${remainingParts.mkString("\n")}
//                |======================
//              """.stripMargin)
//           throw e
//       }
//      droppedParts += partition
//     }
//   }
// }
//
// private[hive] object HuldarHiveClientImpl extends Logging {
//
//
//   def toHiveTableType(catalogTableType: CatalogTableType): HiveTableType = {
//     catalogTableType match {
//       case CatalogTableType.EXTERNAL => HiveTableType.EXTERNAL_TABLE
//       case CatalogTableType.MANAGED => HiveTableType.MANAGED_TABLE
//       case CatalogTableType.VIEW => HiveTableType.VIRTUAL_VIEW
//       case t =>
//         throw new IllegalArgumentException(
//           s"Unknown table type is found at toHiveTableType: $t")
//     }
//   }
//
//   private def toInputFormat(name: String) =
//     Utils.classForName[org.apache.hadoop.mapred.InputFormat[_, _]](name)
//
//   private def toOutputFormat(name: String) =
//     Utils.classForName[org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]](name)
//
//
//   def toHiveTable(table: CatalogTable, userName: Option[String] = None): HiveTable = {
//     val hiveTable = new HiveTable(table.database, table.identifier.table)
//     // 1.设置表类型
//     hiveTable.setTableType(toHiveTableType(table.tableType))
//
//     if (table.tableType == CatalogTableType.EXTERNAL) {
//       hiveTable.setProperty("EXTERNAL", "TRUE")
//     }
//
//     val (partCols, schema) = table.schema.map(toHiveColumn).partition { c =>
//       table.partitionColumnNames.contains(c.getName)
//     }
//     hiveTable.setFields(schema.asJava)
//     hiveTable.setPartCols(partCols.asJava)
//     Option(table.owner).filter(_.nonEmpty).orElse(userName).foreach(hiveTable.setOwner)
//     hiveTable.setCreateTime(MILLISECONDS.toSeconds(table.createTime).toInt)
//     hiveTable.setLastAccessTime(MILLISECONDS.toSeconds(table.lastAccessTime).toInt)
//     table.storage.locationUri.map(CatalogUtils.URIToString).foreach { loc =>
//       hiveTable.getTTable.getSd.setLocation(loc)}
//     table.storage.inputFormat.map(toInputFormat).foreach(hiveTable.setInputFormatClass)
//     table.storage.outputFormat.map(toOutputFormat).foreach(hiveTable.setOutputFormatClass)
//     hiveTable.setSerializationLib(table.storage.serde.getOrElse("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
//     table.storage.properties.foreach { case (k, v) => hiveTable.setSerdeParam(k, v) }
//     table.comment.foreach { c => hiveTable.setProperty("comment", c) }
//     table.viewText.foreach { t =>
//       hiveTable.setViewOriginalText(t)
//       hiveTable.setViewExpandedText(t)
//     }
//
//     table.bucketSpec match {
//       case Some(bucketSpec) if HiveExternalCatalog.isDatasourceTable(table) =>
//         hiveTable.setNumBuckets(bucketSpec.numBuckets)
//         hiveTable.setBucketCols(bucketSpec.bucketColumnNames.toList.asJava)
//
//         if (bucketSpec.sortColumnNames.nonEmpty) {
//           hiveTable.setSortCols(
//             bucketSpec.sortColumnNames
//               .map(col => new Order(col, HIVE_COLUMN_ORDER_ASC))
//               .toList
//               .asJava
//           )
//         }
//       case _ =>
//     }
//     hiveTable
//   }
//   def toHiveColumn(c: StructField): FieldSchema = {
//     val typeString = if (SQLConf.get.charVarcharAsString) {
//       c.dataType.catalogString
//     } else {
//       CharVarcharUtils.getRawTypeString(c.metadata).getOrElse(c.dataType.catalogString)
//     }
//     new FieldSchema(c.name, typeString, c.getComment().orNull)
//   }
//
//   private def verifyColumnDataType(schema: StructType): Unit = {
//     schema.foreach(col => getSparkSqlDataType(toHiveColumn(col)))
//   }
//
//   private val HiveStatisticsProperties = Set(
//     StatsSetupConst.COLUMN_STATS_ACCURATE,
//     StatsSetupConst.NUM_FILES,
//     StatsSetupConst.NUM_PARTITIONS,
//     StatsSetupConst.ROW_COUNT,
//     StatsSetupConst.RAW_DATA_SIZE,
//     StatsSetupConst.TOTAL_SIZE
//   )
//
//
//   private def readHiveStats(properties: Map[String, String]): Option[CatalogStatistics] = {
//     val totalSize = properties.get(StatsSetupConst.TOTAL_SIZE).filter(_.nonEmpty).map(BigInt(_))
//     val rawDataSize = properties.get(StatsSetupConst.RAW_DATA_SIZE).filter(_.nonEmpty)
//       .map(BigInt(_))
//     val rowCount = properties.get(StatsSetupConst.ROW_COUNT).filter(_.nonEmpty).map(BigInt(_))
//
//     if (totalSize.isDefined && totalSize.get > 0) {
//       Some(CatalogStatistics(sizeInBytes = totalSize.get, rowCount = rowCount.filter(_ > 0)))
//     } else if (rawDataSize.isDefined && rawDataSize.get > 0) {
//       Some(CatalogStatistics(sizeInBytes = rawDataSize.get, rowCount = rowCount.filter(_ > 0)))
//     } else {
//       // TODO: still fill the rowCount even if sizeInBytes is empty. Might break anything?
//       None
//     }
//   }
//
//   private def getSparkSqlDataType(hc: FieldSchema): DataType = {
//     try {
//       CatalystSqlParser.parseDataType(hc.getType)
//     } catch {
//       case e: ParseException =>
//         throw QueryExecutionErrors.cannotRecognizeHiveTypeError(e, hc.getType, hc.getName)
//     }
//   }
//
//   def fromHiveColumn(hc: FieldSchema): StructField = {
//     val columnType = getSparkSqlDataType(hc)
//     val field = StructField(
//       name = hc.getName,
//       dataType = columnType,
//       nullable = true)
//     Option(hc.getComment).map(field.withComment).getOrElse(field)
//   }
//
//   def extraFixesForNonView(tTable: MetaStoreApiTable): MetaStoreApiTable = {
//     if (!(HiveTableType.VIRTUAL_VIEW.toString == tTable.getTableType)) {
//       val parameters = tTable.getSd.getParameters
//       if (parameters != null) {
//         val sf = parameters.get(serdeConstants.SERIALIZATION_FORMAT)
//         if (sf != null) {
//           val b: Array[Char] = sf.toCharArray
//           if (b.length == 1 && b(0) < 10) {
//             parameters.put(serdeConstants.SERIALIZATION_FORMAT, Integer.toString(b(0)))
//           }
//         }
//       }
//       // Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
//       // NOTE: LazySimpleSerDe does not support tables with a single column of col
//       // of type "array<string>". This happens when the table is created using
//       // an earlier version of Hive.
//       if (classOf[MetadataTypedColumnsetSerDe].getName ==
//         tTable.getSd.getSerdeInfo.getSerializationLib &&
//         tTable.getSd.getColsSize > 0 &&
//         tTable.getSd.getCols.get(0).getType.indexOf('<') == -1) {
//         tTable.getSd.getSerdeInfo.setSerializationLib(classOf[LazySimpleSerDe].getName)
//       }
//     }
//     tTable
//   }
//
//   def newHiveConf(
//     sparkConf: SparkConf,
//     hadoopConf: JIterable[JMap.Entry[String, String]],
//     extraConfig: Map[String, String],
//     classLoader: Option[ClassLoader] = None): HiveConf = {
//     val hiveConf = new HiveConf(classOf[SessionState])
//     // HiveConf是一个Hadoop Configuration，它有一个classLoader字段，
//     // 初始值会是当前线程的context class loader。
//     // 我们在这里调用 hiveConf.setClassLoader(initClassLoader) 以确保它使用我们想要的类加载器。
//     classLoader.foreach(hiveConf.setClassLoader)
//     // 1: Take all from the hadoopConf to this hiveConf.
//     // This hadoopConf contains user settings in Hadoop's core-site.xml file
//     // and Hive's hive-site.xml file. Note, we load hive-site.xml file manually in
//     // SharedState and put settings in this hadoopConf instead of relying on HiveConf
//     // to load user settings. Otherwise, HiveConf's initialize method will override
//     // settings in the hadoopConf. This issue only shows up when spark.sql.hive.metastore.jars
//     // is not set to builtin. When spark.sql.hive.metastore.jars is builtin, the classpath
//     // has hive-site.xml. So, HiveConf will use that to override its default values.
//     // 2: we set all spark confs to this hiveConf.
//     // 3: we set all entries in config to this hiveConf.
//     val confMap = (hadoopConf.iterator().asScala.map(kv => kv.getKey -> kv.getValue) ++
//       sparkConf.getAll.toMap ++ extraConfig).toMap
//     confMap.foreach { case (k, v) => hiveConf.set(k, v) }
//     SQLConf.get.redactOptions(confMap).foreach { case (k, v) =>
//       logDebug(s"Applying Hadoop/Hive/Spark and extra properties to Hive Conf:$k=$v")
//     }
//     // Disable CBO because we removed the Calcite dependency.
//     hiveConf.setBoolean("hive.cbo.enable", false)
//
//     if (hiveConf.getBoolean("hive.session.history.enabled", false)) {
//       logWarning("Detected HiveConf hive.session.history.enabled is true and will be reset to" +
//         " false to disable useless hive logic")
//       hiveConf.setBoolean("hive.session.history.enabled", false)
//     }
//     // If this is tez engine, SessionState.start might bring extra logic to initialize tez stuff,
//     // which is useless for spark.
//     if (hiveConf.get("hive.execution.engine") == "tez") {
//       logWarning("Detected HiveConf hive.execution.engine is 'tez' and will be reset to 'mr'" +
//         " to disable useless hive logic")
//       hiveConf.set("hive.execution.engine", "mr")
//     }
//
//     hiveConf
//   }
//
//
//   def getHive(conf: Configuration): Hive = {
//     val hiveConf = conf match {
//       case hiveConf: HiveConf =>
//         hiveConf
//       case _ =>
//         new HiveConf(conf, classOf[HiveConf])
//     }
//     try {
//       classOf[Hive].getMethod("getWithoutRegisterFns", classOf[HiveConf])
//         .invoke(null, hiveConf).asInstanceOf[Hive]
//     } catch {
//       // SPARK-37069: not all Hive versions have the above method (e.g., Hive 2.3.9 has it but
//       // 2.3.8 don't), therefore here we fallback when encountering the exception.
//       case _: NoSuchMethodException =>
//         Hive.get(hiveConf)
//     }
//   }
//
// }
//
//
