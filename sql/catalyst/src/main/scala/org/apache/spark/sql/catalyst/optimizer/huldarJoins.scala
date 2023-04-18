/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, GenericInternalRow, NamedExpression, PredicateHelper, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_JOIN

import scala.util.control.NonFatal

object HuldarEliminationJoin extends Rule[LogicalPlan] with PredicateHelper {


  private def canFilterOutNull (e: Expression): Boolean = {
    if (!e.deterministic || SubqueryExpression.hasCorrelatedSubquery(e)) return false
    val attributes = e.references.toSeq
    val emptyRow = new GenericInternalRow(attributes.length)
    val boundE = BindReferences.bindReference(e, attributes)
    if (boundE.exists(_.isInstanceOf[Unevaluable])) return false;

    try {
      val v = boundE.eval(emptyRow)
      v == null || v == false
    } catch {
      case NonFatal(e) =>
        false
    }
  }

  private def buildNewJoinType (filter: Filter, join: Join): JoinType = {
    val conditions = splitConjunctivePredicates(filter.condition) ++ filter.constraints
    val leftConditions = conditions.filter(_.references.subsetOf(join.left.outputSet))
    val rightConditions = conditions.filter(_.references.subsetOf(join.right.outputSet))

    lazy val leftHasNonNullPredicate = leftConditions.exists(canFilterOutNull)
    lazy val rightHasNonNullPredicate = rightConditions.exists(canFilterOutNull)

    join.joinType match {
      case RightOuter if leftHasNonNullPredicate => Inner
      case LeftOuter if leftHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate && rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate => LeftOuter
      case FullOuter if leftHasNonNullPredicate => RightOuter
      case o => o
    }

  }

  def allDuplicateAgnostic (aggExprs: Seq[NamedExpression]): Boolean = {
    aggExprs.exists(_.exists {
      case agg: AggregateFunction => !EliminateDistinct.isDuplicateAgnostic(agg)
      case _ => false
    })
  }

  override def apply (plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(OUTER_JOIN), ruleId) {
    case f @ Filter(condition, j @ Join(_, _, RightOuter | LeftOuter | FullOuter, _, _)) =>
      val newJoinType = buildNewJoinType(f, j)
      if (j.joinType == newJoinType) f else Filter(condition, j.copy(joinType = newJoinType))

    case a @ Aggregate(_, aggExprs, Join(left, _, LeftOuter, _, _))
      if a.references.subsetOf(left.outputSet) && allDuplicateAgnostic(aggExprs) =>
      a.copy(child = left)
    case a @ Aggregate(_, aggExprs, Join(_, right, RightOuter, _, _))
      if a.references.subsetOf(right.outputSet) && allDuplicateAgnostic(aggExprs) =>
      a.copy(child = right)

    case a @ Aggregate(_, aggreExprs, p @ Project(projectList, Join(left, _, LeftOuter, _, _)))
      if projectList.forall(_.deterministic) && p.references.subsetOf(left.outputSet) =>
      plan
    case a @ Aggregate(_, aggreExprs, p @ Project(projectList, Join(left, _, RightOuter, _, _))) =>
      plan
    case p @ Project(_, ExtractEquiJoinKeys(LeftOuter, _, rightKeys, _, _, left, right, _)) =>
      plan
    case p @ Project(_, ExtractEquiJoinKeys(RightOuter, _, rightKeys, _, _, left, right, _)) =>
      plan
  }
}