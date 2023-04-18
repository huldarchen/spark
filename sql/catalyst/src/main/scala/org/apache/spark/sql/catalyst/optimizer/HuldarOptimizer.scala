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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PROJECT, UNION}


object HuldarPushProjectionThroughUnion extends Rule[LogicalPlan] {


  private def buildRewrites (left: LogicalPlan, right: LogicalPlan) = {
    // union 两边字段要一致
    assert(left.output.size == right.output.size)
    AttributeMap(left.output.zip(right.output))
  }

  private def pushToRight[A <: Expression] (e: A, rewrites: AttributeMap[Attribute]) = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    } match {
      case Alias(child, alias) => Alias(child, alias)()
      case other => other
    }
    result.asInstanceOf[A]
  }

  private def pushProjectionThroughUnion (projectList: Seq[NamedExpression], u: Union): Seq[LogicalPlan] = {
    val newFirstChild = Project(projectList, u.children.head)
    val newOtherChildren = u.children.tail.map { child =>
      val rewrites = buildRewrites(u.children.head, child)
      Project(projectList.map(pushToRight(_, rewrites)), child)
    }
    newFirstChild +: newOtherChildren
  }

  override def apply (plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAllPatterns(UNION, PROJECT)) {
    case Project(projectList, u: Union)
      if projectList.forall(_.deterministic) && u.children.nonEmpty =>
      u.copy(children = pushProjectionThroughUnion(projectList, u))
  }
}