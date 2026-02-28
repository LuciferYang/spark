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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ParquetLazyMaterializationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("Lazy materialization should return correct results") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withTempPath { path =>
        val data = (0 until 100).map(i => (i, i * 2, s"str$i"))
        data.toDF("a", "b", "c").write.parquet(path.toString)

        val df = spark.read.parquet(path.toString)
        // Filter that keeps some rows
        checkAnswer(df.filter("a > 50").select("b", "c"),
          data.filter(_._1 > 50).map(r => Row(r._2, r._3)))
          
        // Filter that drops all rows
        checkAnswer(df.filter("a > 1000").select("b"), Seq.empty)
      }
    }
  }
  
  test("Lazy materialization with nested types") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withTempPath { path =>
        val data = (0 until 10).map(i => (i, (i, s"val$i")))
        data.toDF("a", "b").write.parquet(path.toString)
        
        val df = spark.read.parquet(path.toString)
        checkAnswer(df.filter("a = 5").select("b"),
          data.filter(_._1 == 5).map(r => Row(Row(r._2._1, r._2._2))))
      }
    }
  }
}
