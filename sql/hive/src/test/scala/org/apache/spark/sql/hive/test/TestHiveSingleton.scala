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

package org.apache.spark.sql.hive.test

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.client.HiveClient


trait TestHiveSingleton extends SparkFunSuite with BeforeAndAfterAll {
  override protected val enableAutoThreadAudit = false
  protected val spark: SparkSession = TestHive.sparkSession
  protected val hiveContext: TestHiveContext = TestHive
  protected val hiveClient: HiveClient =
    spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

  protected override def afterAll(): Unit = {
    try {
      hiveContext.reset()
    } finally {
      super.afterAll()
    }
  }

  protected override def afterEach(): Unit = {
    try {
      spark.artifactManager.cleanUpResourcesForTesting()
    } finally {
      super.afterEach()
    }
  }
}
