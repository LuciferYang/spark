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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo.KRYO_REGISTRATION_REQUIRED
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.Decimal

/**
 * Regression tests for `DefaultCachedBatchKryoSerializer` round-trip when the
 * cached-batch stats row contains types whose Kryo class registration was
 * incomplete.
 *
 * The motivating failure was hit during a TPC-DS benchmark run of
 * cross-query-reuse queries (q23a -> q23b) on the `sql-optimize+autocte`
 * branch. When memory pressure triggered eviction of a cached CTE block,
 * `DefaultCachedBatchKryoSerializer.write` called
 * `kryo.writeClassAndObject(output, batch.stats)` which dispatched to Kryo's
 * default serializer for `GenericInternalRow`. That serializer then walked
 * the `values: Array[Any]` and tried to register each concrete element
 * class. When the stats row held a `Decimal` value (e.g. from a TPC-DS
 * column with `DecimalType`), Kryo's strict registration mode threw:
 *
 *   KryoException: Class is not registered: org.apache.spark.sql.types.Decimal
 *
 * The fix is to register `org.apache.spark.sql.types.Decimal` (and its array
 * variant, defensively) in `KryoSerializer.loadableSparkClasses`. These tests
 * lock in the round-trip behaviour so the registration cannot silently
 * regress.
 */
class DefaultCachedBatchKryoSerializerSuite extends SparkFunSuite {

  private def newSerializer(): org.apache.spark.serializer.SerializerInstance = {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)
    new KryoSerializer(conf).newInstance()
  }

  test("SPARK-XXXXX: round-trip Decimal in strict Kryo mode") {
    val ser = newSerializer()
    val value = Decimal(123456789L, 10, 2)
    val deserialized = ser.deserialize[Decimal](ser.serialize(value))
    assert(deserialized === value)
  }

  test("SPARK-XXXXX: round-trip GenericInternalRow containing a Decimal") {
    val ser = newSerializer()
    val row = new GenericInternalRow(
      Array[Any](42, Decimal(987654321L, 18, 6), 3.14D))
    val bytes = ser.serialize(row)
    val back = ser.deserialize[GenericInternalRow](bytes)
    // Field-wise equality: primitives survive identity, Decimal is compared
    // via its own equals.
    assert(back.numFields === 3)
    assert(back.getInt(0) === 42)
    assert(back.getDecimal(1, 18, 6) === Decimal(987654321L, 18, 6))
    assert(back.getDouble(2) === 3.14D)
  }

  test("SPARK-XXXXX: round-trip DefaultCachedBatch with Decimal stats " +
       "(cached-batch spill path)") {
    val ser = newSerializer()
    // Mimics the shape produced by `DefaultCachedBatchSerializer` for a
    // column whose stats include Decimal min/max values.
    val stats = new GenericInternalRow(
      Array[Any](Decimal(0L, 10, 2), Decimal(100000000L, 10, 2), 0L, 0L))
    val batch = DefaultCachedBatch(
      numRows = 10,
      buffers = Array(Array[Byte](1, 2, 3), Array[Byte](4, 5, 6)),
      stats = stats)
    val bytes = ser.serialize(batch)
    val back = ser.deserialize[DefaultCachedBatch](bytes)
    assert(back.numRows === 10)
    assert(back.buffers.length === 2)
    assert(back.buffers(0).toSeq === Seq[Byte](1, 2, 3))
    assert(back.buffers(1).toSeq === Seq[Byte](4, 5, 6))
    assert(back.stats.getDecimal(0, 10, 2) === Decimal(0L, 10, 2))
    assert(back.stats.getDecimal(1, 10, 2) === Decimal(100000000L, 10, 2))
  }

  test("SPARK-XXXXX: round-trip Array[Decimal] in strict Kryo mode") {
    val ser = newSerializer()
    val arr: Array[Decimal] = Array(
      Decimal(1L, 5, 0),
      Decimal(2L, 5, 0),
      Decimal(3L, 5, 0))
    val back = ser.deserialize[Array[Decimal]](ser.serialize(arr))
    assert(back.toSeq === arr.toSeq)
  }
}
