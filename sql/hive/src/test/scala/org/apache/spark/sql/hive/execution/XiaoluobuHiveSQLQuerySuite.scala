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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

case class OrderInfo(
  id: Int,
  make: String,
  `type`: String,
  price: Int,
  pdate: String,
  customer: String,
  city: String,
  state: String,
  dt: Int)

class XiaoluobuHiveSQLQuerySuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  import spark.implicits._

  test("SPARK-6851: Self-joined converted parquet tables") {
    withTempView("orders1") {
      val orderInfo = Seq(
        OrderInfo(1, "Atlas", "MTB", 234, "2015-01-07", "John D", "Pacifica", "CA", 202211),
        OrderInfo(3, "Swift", "MTB", 285, "2015-01-17", "John S", "Redwood City", "CA", 20221101),
        OrderInfo(4, "Atlas", "Hybrid", 303, "2015-01-23", "Jones S", "San Mateo", "CA", 20221102),
        OrderInfo(7, "Next", "MTB", 356, "2015-01-04", "Jane D", "Daly City", "CA", 20221103),
        OrderInfo(10, "Next", "YFlikr", 187, "2015-01-09", "John D", "Fremont", "CA", 20221104),
        OrderInfo(11, "Swift", "YFlikr", 187, "2015-01-23", "John D", "Hayward", "CA", 20221105),
        OrderInfo(2, "Next", "Hybrid", 324, "2015-02-03", "Jane D", "Daly City", "CA", 20221106),
        OrderInfo(5, "Next", "Street", 187, "2015-02-08", "John D", "Fremont", "CA", 20221107),
        OrderInfo(6, "Atlas", "Street", 154, "2015-02-09", "John D", "Pacifica", "CA", 20221108),
        OrderInfo(8, "Swift", "Hybrid", 485, "2015-02-19", "John S", "Redwood City", "CA", 20221109),
        OrderInfo(9, "Atlas", "Split", 303, "2015-02-28", "Jones S", "San Mateo", "CA", 20221110))

      this.sparkContext.setLogLevel("ERROR")
      // this.sparkContext.setLogLevel("INFO")

      orderInfo.toDF.createOrReplaceTempView("orders1")

      withTable("orders") {
        sql("CREATE TABLE orders (id INT, make String,type String,price INT,pdate String,customer String,city String, state STRING ) PARTITIONED BY (dt INT) STORED AS PARQUET")
        sql("set hive.exec.dynamic.partition.mode=nonstrict")
        sql("INSERT INTO TABLE orders PARTITION (dt) SELECT * FROM orders1")
        // val analyzedDF =  sql(" select * from orders")

        sql("CREATE TEMPORARY FUNCTION pre_week_end_date_4_ads AS 'com.dmall.udf.UDFPreWeekEndDate';")
        sql("CREATE TEMPORARY FUNCTION pre_week_start_date_4_ads AS 'com.dmall.udf.UDFPreWeekStartDate';")

        val analyzedDF = sql(" SELECT * FROM orders WHERE dt BETWEEN pre_week_end_date_4_ads('20221112') AND pre_week_end_date_4_ads('20221112', 1)")

        // analyzedDF.show(false)
        val ana = analyzedDF.queryExecution.analyzed
        println("== Analyzed Logical Plan ==")
        println(ana)
        // println( ana.prettyJson)
        println("== Optimized Logical Plan ==")
        val opt = analyzedDF.queryExecution.optimizedPlan
        println(opt)
        // println( opt.prettyJson)
        println("== Physical Plan ==")
        val phy = analyzedDF.queryExecution.sparkPlan
        println(phy)
        // println( phy.prettyJson)
        println("== executedPlan ==")
        println(analyzedDF.queryExecution.executedPlan)
        analyzedDF.show(false)
      }
    }
  }
}
