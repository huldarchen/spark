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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the given logical operation then an
 * empty list should be returned.
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

/**
 * Abstract class for transforming [[LogicalPlan]]s into physical plans.
 * Child classes are responsible for specifying a list of [[GenericStrategy]] objects that
 * each of which can return a list of possible physical plan options.
 * If a given strategy is unable to plan all of the remaining operators in the tree,
 * it can call [[GenericStrategy#planLater planLater]], which returns a placeholder
 * object that will be [[collectPlaceholders collected]] and filled in
 * using other available strategies.
 *
 * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
 *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // Collect physical plan candidates.
    // val candidates = strategies.iterator.flatMap(_(plan))
    val candidates = strategies.iterator.flatMap(sta => {
      println(sta.getClass.getSimpleName)
      val plans = sta.apply(plan)
      plans.foreach(p => p.toJSON)
      plans
    })
    // println(s"当前逻辑执行计划: $plan")

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    // SR2 [physical] 逻辑在这里 这里做了什么呢??? 要仔细看下
    val plans = candidates.flatMap { candidate =>
      // SR2 [physical] 这个flatMap是什么时候调用的呢?  assert(pruned.hasNext, s"No plan for $plan")的还是调用
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        // println(s"候选者是空: $candidate")
        Iterator(candidate)
      } else {
        // println(s"placeholders+++ : ${placeholders.head._1}  +=++ ${placeholders.head._2}")
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // println(s"处理的下一个逻辑执行计划: $logicalPlan")
            // println(s"处理的下一个物理执行计划: $placeholder")
            // Plan the logical plan for the placeholder.
            // SR2 [physical] 物理计划作用于子节点
            val childPlans = this.plan(logicalPlan)
            // SR2 [physical] 这个是递归以后执行的
            // SR2 [physical] QueryExecution.creatPhysicalPlan.next才会触发flatMap中的方法????感觉不对呢
            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                // SR2 [physical] 自下而上的替换
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) =>
                    // println(s"当前的物理执行计划: $childPlan")
                    // println(s"当前候选并且包含占位符的执行计划: $candidateWithPlaceholders")
                    childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  /**
   * Collects placeholders marked using [[GenericStrategy#planLater planLater]]
   * by [[strategies]].
   */
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}
