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
 *
 */

package org.apache.spark.sql

object HuldarChenTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    import spark.implicits._
    Seq(
      (20220801, "A", 1, 202208),
      (20220801, "B", 1, 202208),
      (20220802, "B", 1, 202208),
      (20220803, "C", 1, 202208)
    ).toDF("dt", "ct", "metric", "mdt")
      .createOrReplaceTempView("test1")

    val df = spark.sql(
      """
        | SELECT
        |  dt, ct,
        |  ROW_NUMBER() over (partition by mdt, ct order by dt) as row_number,
        |  sum(metric) over (partition by mdt, ct order by dt) as sum_metric
        | FROM
        | test1
      """.stripMargin)

    val executedPlan = df.queryExecution.executedPlan
    println(executedPlan.prettyJson)

    df.show(truncate = false)

  }

}
