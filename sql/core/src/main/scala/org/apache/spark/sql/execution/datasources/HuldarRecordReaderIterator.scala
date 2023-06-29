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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.mapreduce.RecordReader
import org.apache.spark.sql.errors.QueryExecutionErrors

import java.io.Closeable

class HuldarRecordReaderIterator[T](
    private[this] var rowReader: RecordReader[_, T]) extends Iterator[T] with Closeable {
  private[this] var havePair = false
  private[this] var finished = false

  override def hasNext: Boolean = {
    if (!finished && !havePair) {
      finished =  !rowReader.nextKeyValue()
      if (finished) {
        close()
      }
      havePair = !finished
    }
    !finished
  }

  override def next(): T = {
    if (!hasNext) {
      throw QueryExecutionErrors.endOfStreamError()
    }
    havePair = false
    rowReader.getCurrentValue
  }


  override def map[B](f: T => B): Iterator[B] with Closeable =
    new Iterator[B] with Closeable {
      override def hasNext: Boolean = HuldarRecordReaderIterator.this.hasNext
      override def next(): B = f(HuldarRecordReaderIterator.this.next())
      override def close(): Unit = HuldarRecordReaderIterator.this.close()
    }

  override def close(): Unit = {
    if (rowReader != null) {
      try {
        rowReader.close()
      } finally {
        rowReader = null
      }
    }
  }
}
