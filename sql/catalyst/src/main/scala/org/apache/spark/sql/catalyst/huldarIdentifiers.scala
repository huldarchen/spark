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

package org.apache.spark.sql.catalyst


sealed trait HuldarCatalystIdentifier {
  val identifier: String

  def database: Option[String]

  def catalog: Option[String]

  // ` => ``
  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  def quotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier)
    val replacedCatalog = catalog.map(quoteIdentifier)

    if (replacedCatalog.isDefined && replacedDb.isDefined) {
      s"`${replacedCatalog.get}`.`${replacedDb.get}`.`$replacedId`"
    } else if (replacedDb.isDefined) {
      s"`${replacedDb.get}`.`$replacedId`"
    } else {
      s"`$replacedId`"
    }
  }

  def unquotedString: String = {
    if (catalog.isDefined && database.isDefined) {
      s"${catalog.get}.${database.get}.$identifier"
    } else if (database.isDefined) {
      s"${database.get}.$identifier"
    } else {
      identifier
    }
  }

  def nameParts: Seq[String] = {
    if (catalog.isDefined && database.isDefined) {
      Seq(catalog.get, database.get, identifier)
    } else if (database.isDefined) {
      Seq(database.get, identifier)
    } else {
      Seq(identifier)
    }
  }

  override def toString: String = quotedString
}


case class HuldarAliasIdentifier(name: String, qualifier: Seq[String]) {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def this(identifier: String) = this(identifier, Seq())

  override def toString: String = (qualifier :+ name).quoted
}

object HuldarAliasIdentifier {
  def apply(name: String): HuldarAliasIdentifier = new HuldarAliasIdentifier(name)
}


case class HuldarTableIdentifier(table: String, database: Option[String], catalog: Option[String])
  extends HuldarCatalystIdentifier {

  assert(catalog.isEmpty || database.isDefined)

  override val identifier: String = table

  def this(table: String) = this(table, None, None)

  def this(table: String, database: Option[String]) = this(table, database, None)
}

object HuldarTableIdentifier {
  def apply(table: String): HuldarTableIdentifier = new HuldarTableIdentifier(table)

  def apply(table: String, database: Option[String]) = new HuldarTableIdentifier(table, database)
}

/**
 * Identifies a function in a database.
 * If `database` is not defined, the current database is used.
 */
case class HuldarFunctionIdentifier(funcName: String, database: Option[String], catalog: Option[String])
  extends HuldarCatalystIdentifier {

  assert(catalog.isEmpty || database.isDefined)

  override val identifier: String = funcName

  def this(funcName: String) = this(funcName, None, None)

  def this(FuncName: String, database: Option[String]) = this(FuncName, database, None)

  override def toString: String = unquotedString
}

object HuldarFunctionIdentifier {
  def apply(funcName: String): HuldarFunctionIdentifier = new HuldarFunctionIdentifier(funcName)

  def apply(funcName: String, database: Option[String]): HuldarFunctionIdentifier = new HuldarFunctionIdentifier(funcName, database)

}




