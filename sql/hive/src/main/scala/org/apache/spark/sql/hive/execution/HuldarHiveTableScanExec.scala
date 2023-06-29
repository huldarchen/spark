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

package org.apache.spark.sql.hive.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

private[hive]
case class HuldarHiveTableScanExec(
  requestedAttributes: Seq[Attribute],
  relation: HiveTableRelation,
  partitionPruningPred: Seq[Expression])(
  @transient private val sparkSession: SparkSession)
  extends LeafExecNode with CastSupport {

  require(partitionPruningPred.isEmpty || relation.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")


  override def conf: SQLConf = sparkSession.sessionState.conf

  override def nodeName: String = s"Scan hive ${relation.tableMeta.qualifiedName}"


  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  )


  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPred.flatMap(_.references))

  private val originalAttributes = AttributeMap(relation.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    requestedAttributes.map(originalAttributes)
  }


  private lazy val boundPruningPre = partitionPruningPred.reduceLeftOption(And).map { pred =>
    require(pred.dataType == BooleanType,
      s"Data type of predicate $pred must be ${BooleanType.catalogString} rather than " +
      s"${pred.dataType.catalogString}.")

    BindReferences.bindReference(pred, relation.partitionCols)
  }

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(relation.tableMeta)

  override protected def doExecute(): RDD[InternalRow] = ???

  override def productElement(n: Int): Any = ???

  override def productArity: Int = ???

  override def canEqual(that: Any): Boolean = ???
}
