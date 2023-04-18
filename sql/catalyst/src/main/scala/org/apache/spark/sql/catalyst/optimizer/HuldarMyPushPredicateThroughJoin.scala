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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, ExpressionSet, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.FILTER

object HuldarPushPredicateThroughNonJoin extends Rule[LogicalPlan] with PredicateHelper {
  override def apply (plan: LogicalPlan): LogicalPlan = plan transform applyLocally


  def canPushThrough (p: UnaryNode): Boolean = p match {
    case _: AppendColumns => true
    case _: Distinct => true
    case _: Generate => true
    case _: Pivot => true
    case _: RepartitionByExpression => true
    case _: Repartition => true
    case _: RebalancePartitions => true
    case _: ScriptTransformation => true
    case _: Sort => true
    case _: BatchEvalPython => true
    case _: ArrowEvalPython => true
    case _: Expand => true
    case _ => false
  }


  def pushDownPredicate (
    filter: Filter,
    grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
    val (candidates, nonDeterministic) = splitConjunctivePredicates(filter.condition).partition(_.deterministic)
    val (pushDown, rest) = candidates.partition { cond =>
      cond.references.subsetOf(grandchild.outputSet)
    }
    val stayUp = rest ++ nonDeterministic

    if (pushDown.nonEmpty) {
      val newChild = insertFilter(pushDown.reduce(And))
      if (stayUp.nonEmpty) Filter(stayUp.reduce(And), newChild) else newChild
    } else {
      filter
    }

  }

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    case Filter(condition, project @ Project(fields, grandChild))
      if fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition) =>
      val aliasMap = getAliasMap(project)
      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))

    case filter @ Filter(condition, aggregate: Aggregate)
      if aggregate.aggregateExpressions.forall(_.deterministic)
        && aggregate.groupingExpressions.nonEmpty =>

      val aliasMap = getAliasMap(aggregate)
      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)
      val (pushDown, rest) = candidates.partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
      }
      val stayUp = rest ++ nonDeterministic
      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val replaced = replaceAlias(pushDownPredicate, aliasMap)
        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
        if (stayUp.isEmpty) newAggregate else Filter(stayUp.reduce(And), newAggregate)
      } else {
        filter
      }

    case filter @ Filter(condition, w: Window)
      if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))
      val (candidates, nonDeterministic) = splitConjunctivePredicates(condition).partition(_.deterministic)
      val (pushDown, rest) = candidates.partition { cond =>
        cond.references.subsetOf(partitionAttrs)
      }
      val stayUp = rest ++ nonDeterministic
      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newWidow = w.copy(child = Filter(pushDownPredicate, w.child))
        if (stayUp.isEmpty) newWidow else Filter(stayUp.reduce(And), newWidow)
      } else {
        filter
      }

    case filter @ Filter(condition, union: Union) =>
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition(_.deterministic)
      if (pushDown.nonEmpty) {
        val pushDownCond = pushDown.reduce(And)
        val output = union.output
        val newGrandChildren = union.children.map { grandchild =>
          val newCond = pushDownCond transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandchild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandchild.outputSet))
          Filter(newCond, grandchild)
        }
        val newUnion = union.withNewChildren(newGrandChildren)
        if (stayUp.nonEmpty) {
          Filter(stayUp.reduce(And), newUnion)
        } else {
          newUnion
        }
      } else {
        filter
      }

    case filter @ Filter(condition, watermark: EventTimeWatermark) =>
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition { p =>
        p.deterministic && !(p.references.contains(watermark.eventTime))
      }

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newWaterMark = watermark.copy(child = Filter(pushDownPredicate, watermark.child))
        if (stayUp.isEmpty) newWaterMark else Filter(stayUp.reduce(And), newWaterMark)
      } else {
        filter
      }

    case filter @ Filter(_, u: UnaryNode)
      if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      pushDownPredicate(filter, u.child) { predicate =>
        u.withNewChildren(Seq(Filter(predicate, u.child)))
      }
  }

  private def canPushThroughCondition (plan: LogicalPlan, condition: Expression): Boolean = {
    val attributes = plan.outputSet
    !condition.exists {
      case s: SubqueryExpression => s.plan.outputSet.intersect(attributes).nonEmpty
      case _ => false
    }
  }

}

object HuldarMyPushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {

  override def apply (plan: LogicalPlan): LogicalPlan = plan transform applyLocally

  private def canPushThrough (joinType: JoinType) = joinType match {
    case _: InnerLike | LeftSemi | RightOuter | LeftOuter | LeftAnti | ExistenceJoin(_) => true
    case _ => false
  }


  def split (condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (pushDownCandidates, noDeterministic) = condition.partition(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
      rest.partition(_.references.subsetOf(right.outputSet))
    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ noDeterministic)
  }

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    // join后谓词下推
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition, hint))
      if canPushThrough(joinType) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)
      joinType match {
        case _: InnerLike =>
          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val (newJoinCondition, others) = commonFilterCondition.partition(canEvaluateWithinJoin)
          val newJoinCond = (newJoinCondition ++ joinCondition).reduceLeftOption(And)
          val join = Join(newLeft, newRight, joinType, newJoinCond, hint)
          if (others.nonEmpty) {
            Filter(others.reduceLeft(And), join)
          } else {
            join
          }
        case LeftOuter | LeftExistence(_) =>
          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond, hint)
          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case RightOuter =>
          val newLeft = left
          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond, hint)
          (leftFilterConditions ++ commonFilterCondition)
            .reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case other =>
          throw new IllegalStateException(s"Unexpected join type: $other")
      }
    // join中谓词下推
    case j @ Join(left, right, joinType, joinCondition, hint) if canPushThrough(joinType) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _: InnerLike | LeftSemi =>
          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case RightOuter =>
          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case other =>
          throw new IllegalStateException(s"Unexpected join type: $other")
      }
  }

  object HudldarCombineFilters extends Rule[LogicalPlan] with PredicateHelper {
    def apply (plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
      _.containsPattern(FILTER), ruleId)(myApplyLocally)

    val myApplyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
      case Filter(fc, nf @ Filter(nc, grandChild)) if nc.deterministic =>
        val (combineCandidates, nonDeterministic) =
          splitConjunctivePredicates(fc).partition(_.deterministic)
        val mergedFilter = (ExpressionSet(combineCandidates) --
          ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
          case Some(ac) =>
            Filter(And(nc, ac), grandChild)
          case None =>
            nf
        }
        nonDeterministic.reduceOption(And).map(c => Filter(c, mergedFilter)).getOrElse(mergedFilter)
    }

  }

}
