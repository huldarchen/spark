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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.collection.Utils

object HuldarRewriteDistinctAggregates extends Rule[LogicalPlan] {

  private def mayNeedtoRewrite(a: Aggregate): Boolean = {
    val aggExpressions = collectAggregateExprs(a)
    val distinctAggs = aggExpressions.filter(_.isDistinct)
    distinctAggs.size > 1 || distinctAggs.exists(_.filter.isDefined)
  }



  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(AGGREGATE)) {
    case a: Aggregate if mayNeedtoRewrite(a) => rewrite(a)
  }

  private def rewrite(a: Aggregate): Aggregate = {
    val aggExpressions = collectAggregateExprs(a)
    val distinctAggs = aggExpressions.filter(_.isDistinct)

    // 提取去重的聚合函数表达式
    // 去重的聚合函数,按照表达式集合进行分组,如果是不可折叠的就直接返回, 如果是可折叠的就返回第一个子节点
    // 这个是用来做什么呢?
    val distinctAggGroups = aggExpressions.filter(_.isDistinct).groupBy { e =>
      val unfoldableChildren = ExpressionSet(e.aggregateFunction.children.filter(!_.foldable))
      if (unfoldableChildren.nonEmpty) {
        // 不可折叠的子节点, count(distinct a), count(distinct b) 这个返回的是 a, b
        unfoldableChildren
      } else {
        // 如果聚合函数的子节点都是可折叠的,例如: count(distinct 1)
        ExpressionSet(e.aggregateFunction.children.take(1))
      }
    }

    if (distinctAggGroups.size > 1 || distinctAggs.exists(_.filter.isDefined)) {

      val gid = AttributeReference("gid", IntegerType, nullable = false)()
      val groupByMap = a.groupingExpressions.collect {
        case ne: NamedExpression => ne -> ne.toAttribute
        case e => e -> AttributeReference(e.sql, e.dataType, e.nullable)()
      }

      val groupByAttrs = groupByMap.map(_._2)

      def patchAggregateFunctionChildren(
        af: AggregateFunction)(
        attrs: Expression => Option[Expression]): AggregateFunction = {
        val newChildren = af.children.map(c => attrs(c).getOrElse(c))
        af.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
      }

      val distinctAggChildren = distinctAggGroups.keySet.flatten.toSeq.distinct
      val distinctAggChildAttrMap = distinctAggChildren.map { e =>
        e.canonicalized -> AttributeReference(e.sql, e.dataType)()
      }

      val distinctAggChildAttrs = distinctAggChildAttrMap.map(_._2)
      val (distinctAggFilters, distinctAggFilterAttrs, maxConds) = distinctAggs.collect {
        case AggregateExpression(_, _, _, filter, _) if filter.isDefined =>
          val (e, attr) = expressionAttributePair(filter.get)
          val aggregateExpr = Max(attr).toAggregateExpression()
          (e, attr, Alias(aggregateExpr, attr.name)())
      }.unzip3

      val distinctAggChildAttrLookup = distinctAggChildAttrMap.filter(!_._1.foldable).toMap
      val distinctAggFilterAttrLookup = Utils.toMap(distinctAggFilters, maxConds.map(_.toAttribute))

      val distinctAggOperatorMap = distinctAggGroups.toSeq.zipWithIndex.map {
        case ((group, expressions), i) =>
          val id = Literal(i + 1)

          val filters = expressions.filter(_.filter.isDefined).map(_.filter.get)
          val filterProjection = distinctAggFilters.map {
            case e if filters.contains(e) => e
            case e => nullify(e)
          }

          val projection = distinctAggChildren.map {
            case e if group.contains(e) => e
            case e => nullify(e)
          } :+ id

          val operators = expressions.map { e =>
            val af = e.aggregateFunction
            val condition = e.filter.flatMap(distinctAggFilterAttrLookup.get)
            val naf = if (af.children.forall(_.foldable)) {
              af
            } else {
              patchAggregateFunctionChildren(af) { x =>
                distinctAggChildAttrLookup.get(x.canonicalized)
              }
            }
            val newCondition = if (condition.isDefined) {
              And(EqualTo(gid, id), condition.get)
            } else {
              EqualTo(gid, id)
            }
            (e, e.copy(aggregateFunction = naf, isDistinct = false, filter = Some(newCondition)))
          }
          (projection ++ filterProjection, operators)
      }

      val regularAggExprs = aggExpressions
        .filter(e => !e.isDistinct && e.children.exists(!_.foldable))
      val regularAggFunChildren = regularAggExprs
        .flatMap(_.aggregateFunction.children.filter(!_.foldable))
      val regularAggFilterAttrs = regularAggExprs.flatMap(_.filterAttributes)
      val regularAggChildren = (regularAggFunChildren ++ regularAggFilterAttrs).distinct
      val regularAggChildAttrMap = regularAggChildren.map(expressionAttributePair)

      val regularGroupId = Literal(0)
      val regularAggChildAttrLookup = regularAggChildAttrMap.toMap
      val regularAggOperatorMap = regularAggExprs.map { e =>
        val af = patchAggregateFunctionChildren(e.aggregateFunction)(regularAggChildAttrLookup.get)
        val filterOpt = e.filter.map(_.transform {
          case a: Attribute => regularAggChildAttrLookup.getOrElse(a, a)
        })
        val operator = Alias(e.copy(aggregateFunction = af, filter = filterOpt), e.sql)()
        val result = aggregate.First(operator.toAttribute, ignoreNulls = true)
          .toAggregateExpression(isDistinct = false, filter = Some(EqualTo(gid, regularGroupId)))
        val resultWithDefault = af.defaultResult match {
          case Some(lit) => Coalesce(Seq(result, lit))
          case None => result
        }
        (e, operator, resultWithDefault)
      }

      val regularAggProjection = if (regularAggExprs.nonEmpty) {
        Seq(a.groupingExpressions ++
          distinctAggChildren.map(nullify) ++
          Seq(regularGroupId) ++
          distinctAggFilterAttrs.map(nullify) ++
          regularAggChildren)
      } else {
        Seq.empty[Seq[Expression]]
      }
      val regularAggNulls = regularAggChildren.map(nullify)
      val distinctAggProjection = distinctAggOperatorMap.map {
        case (projection, _) =>
          a.groupingExpressions ++
            projection ++
            regularAggNulls
      }

      val expand = Expand(
        regularAggProjection ++ distinctAggProjection,
        groupByAttrs ++ distinctAggChildAttrs ++ Seq(gid) ++ distinctAggFilterAttrs ++
          regularAggChildAttrMap.map(_._2),
        a.child)

      val firstAggregateGroupBy = groupByAttrs ++ distinctAggChildAttrs :+ gid
      val firstAggregate = Aggregate(
        firstAggregateGroupBy,
        firstAggregateGroupBy ++ maxConds ++ regularAggOperatorMap.map(_._2),
        expand)

      val transformations: Map[Expression, Expression] =
        (distinctAggOperatorMap.flatMap(_._2) ++
          regularAggOperatorMap.map(e => (e._1, e._3))).toMap

      val patchedAggExpression = a.aggregateExpressions.map { e =>
        e.transformDown {
          case e: Expression =>
            groupByMap
              .find(ge => e.semanticEquals(ge._1))
              .map(_._2)
              .getOrElse(transformations.getOrElse(e, e))
        }.asInstanceOf[NamedExpression]
      }
      Aggregate(groupByAttrs, patchedAggExpression, firstAggregate)
    } else {
      a
    }
  }

  private def nullify(e: Expression) = Literal.create(null, e.dataType)
  private def expressionAttributePair(e: Expression) =
    e -> AttributeReference(e.sql, e.dataType, nullable = true)()

  private def collectAggregateExprs(a: Aggregate): Seq[AggregateExpression] = {
    a.aggregateExpressions.flatMap {
      _.collect {
        case ae: AggregateExpression => ae
      }}
  }

}
