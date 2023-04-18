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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

class XiaoluobuSQLQuerySuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {


  setupTestData()


  test("logical test") {
    /* val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    val df = spark.read.json("examples/src/main/resources/people.json")
     df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
     sqlDF.show()  */
    // val analyzedDF = sql("select b,count(a) as a1,sum(a+b) as a2,sum(b+a) as a3,sum(a)+1 as a4  from `testdata2` as t1 where b<2 group by b")
    // val analyzedDF = sql("select a, sum(b)   from `testdata2` as t1 group by a")
    // val analyzedDF = sql("select tmp1.a,tmp1.b from testdata2 tmp1 join testdata2 tmp2 on tmp1.a=tmp2.a where tmp1.b<1 and tmp2.b>3")
    val analyzedDF = sql("select a,b from (select  A,B,count(1) as c from testdata2 where a>2  group by a,b)tmp where c=1 and b<5")
    // course: String, year: Int, earnings: Double
    spark.sqlContext.setConf("spark.yarn.maxAppAttempts", "1")
    // val analyzedDF = sql(
    //   """
    //     |select a,b
    //     |from testdata2
    //     |where a between  date_format('2022-11-12', 'yMMdd') AND  date_format('2022-11-12', 'yMMdd')
    //     |""".stripMargin)
    // SELECT collect_set (t.c) AS a
    // FROM TaBlE2 t
    // GROUP BY t.a
    // HAVING size (a) > 1
    // val analyzedDF = sql(
    //   """
    //     | SELECT SUM(a) AS b, CAST('2020-01-01' AS DATE) AS fake FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY b HAVING b > 10;
    //   """.stripMargin)
    // val analyzedDF = sql(
    //   """
    //     |select collect_set(testdata2.b) as a
    //     |from testdata2
    //     |group by b
    //     |having size(a) > 0
    //     |""".stripMargin)
    println(analyzedDF.logicalPlan)
    // val analyzedDF = sql("select a, count(distinct b), sum(b)   from `testdata2` as t1 group by a")
    // val analyzedDF = sql("select course, count(distinct year) as year_cnt, count(distinct earnings) earnings_cnt from courseSales group by course")
    // // val analyzedDF = sql("select  date_format(current_date(), '%Y-%m-%d')")
    // val analyzedDF = sql("select b,a from testdata2 where b<2")
    // val analyzedDF = sql("with tmp as (select a from testdata2) select a from tmp union  select a from tmp")
    val ana = analyzedDF.queryExecution.analyzed
    // println("== Analyzed Logical Plan ==")
    println(ana)
    // // println( ana.prettyJson)
    // println("== Optimized Logical Plan ==")
    val opt = analyzedDF.queryExecution.optimizedPlan
    println(opt)
    // println( opt.prettyJson)
    // println("== Physical Plan ==")
    println("---------------------------------------")
    val phy = analyzedDF.queryExecution.sparkPlan
    // analyzedDF.show()
    // println( phy.prettyJson)
    println(s"物理执行计划:\n $phy")
    // println(s"优化后的物理执行计划:\n ${analyzedDF.queryExecution.executedPlan}")
    // println(phy.prettyJson)
    // println("== executedPlan ==")
    println(analyzedDF.queryExecution.executedPlan.prettyJson)
  }


  test("UnsafeRow test") {
    val analyzedDF = sql("select b,count(a) as a1,sum(a+b) as a2,sum(b+a) as a3,sum(a)+1 as a4  from `testdata2` as t1 where b<2 group by b")
    analyzedDF.queryExecution.toRdd.collect().map {
      x =>
        val unsafeRow = x.asInstanceOf[UnsafeRow]
        val numbFields = unsafeRow.numFields()
        val sizeInBytes = unsafeRow.getSizeInBytes
        val baseObject = unsafeRow.getBytes
        val baseOffset = unsafeRow.getBaseOffset
        println("----------")
        println("Num fields: " + numbFields)
        println("Size in bytes: " + sizeInBytes)
        println("Base offset: " + baseOffset)
        x
    }
  }


  test("SPARK-19059: read file based table whose name starts with underscore") {
    withTable("_tbl") {
      sql("CREATE TABLE `_tbl`(i INT) USING parquet")
      //val analyzedDF =sql("INSERT INTO `_tbl` VALUES (1), (2), (3)")
      val analyzedDF = sql("show create table _tbl")
      val ana = analyzedDF.queryExecution.analyzed
      println("== Analyzed Logical Plan ==")
      println(ana)
      // println( ana.prettyJson)
      println("== Optimized Logical Plan ==")
      val opt = analyzedDF.queryExecution.optimizedPlan
      println(opt)
      // println( opt.prettyJson)
      println("== Physical Plan ==")
      println(analyzedDF.queryExecution.sparkPlan)
      println("== executedPlan ==")
      println(analyzedDF.queryExecution.executedPlan)
    }
  }

  test("string sql") {
    val df = sql("select *  from `nullStrings`")
    println(df.queryExecution.executedPlan.prettyJson)
    val analyzedDF: Unit = df.show()
    df.queryExecution.toRdd.foreach {
      row =>
        val unsafeRow = row.asInstanceOf[UnsafeRow]
        val bytes = unsafeRow.getBytes
        println()
    }
  }


}