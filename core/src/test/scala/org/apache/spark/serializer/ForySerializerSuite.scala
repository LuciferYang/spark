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

package org.apache.spark.serializer

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{SharedSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Fory._
import org.apache.spark.internal.config.SERIALIZER
import org.apache.spark.serializer.ForySerializerSuite._
import org.apache.spark.util.ArrayImplicits._

class ForySerializerSuite extends SparkFunSuite with SharedSparkContext {
  conf.set(SERIALIZER, "org.apache.spark.serializer.ForySerializer")
  conf.set(FORY_USE_UNSAFE, false)

  test("basic types") {
    val ser = new ForySerializer(conf).newInstance()
    def check[T: ClassTag](t: T): Unit = {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }
    check(1)
    check(1L)
    check(1.0f)
    check(1.0)
    check(1.toByte)
    check(1.toShort)
    check("")
    check("hello")
    check(Integer.MAX_VALUE)
    check(Integer.MIN_VALUE)
    check(java.lang.Long.MAX_VALUE)
    check(java.lang.Long.MIN_VALUE)
    check[String](null)
    check(Array(1.toByte))
    check(Array(1, 2, 3))
    check(Array(1L, 2L, 3L))
    check(Array(1.0, 2.0, 3.0))
    check(Array(1.0f, 2.9f, 3.9f))
    check(Array("aaa", "bbb", "ccc"))
    check(Array("aaa", "bbb", null))
    check(Array(true, false, true))
    check(Array('a', 'b', 'c'))
    check(Array.empty[Int])
    check(Array(Array("1", "2"), Array("1", "2", "3", "4")))
    check(Array(Array(1.toByte)))
  }

  test("pairs") {
    val ser = new ForySerializer(conf).newInstance()
    def check[T: ClassTag](t: T): Unit = {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }
    check((1, 1))
    check((1, 1L))
    check((1L, 1))
    check((1L, 1L))
    check((1.0, 1))
    check((1, 1.0))
    check((1.0, 1.0))
    check((1.0, 1L))
    check((1L, 1.0))
    check(("x", 1))
    check(("x", 1.0))
    check(("x", 1L))
    check((1, "x"))
    check((1.0, "x"))
    check((1L, "x"))
    check(("x", "x"))
  }

  test("Scala data structures") {
    val ser = new ForySerializer(conf).newInstance()
    def check[T: ClassTag](t: T): Unit = {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }
    check(List[Int]())
    check(List[Int](1, 2, 3))
    check(Seq[Int](1, 2, 3))
    check(List[String]())
    check(List[String]("x", "y", "z"))
    check(None)
    check(Some(1))
    check(Some("hi"))
    check(mutable.ArrayBuffer(1, 2, 3))
    check(mutable.ArrayBuffer("1", "2", "3"))
    check(mutable.Map())
    check(mutable.Map(1 -> "one", 2 -> "two"))
    check(mutable.Map("one" -> 1, "two" -> 2))
    check(mutable.HashMap(1 -> "one", 2 -> "two"))
    check(mutable.HashMap("one" -> 1, "two" -> 2))
    check(List(Some(mutable.HashMap(1 -> 1, 2 -> 2)), None, Some(mutable.HashMap(3 -> 4))))
  }

  test("configuration limits") {
    val bufferSizeProperty = FORY_SERIALIZER_BUFFER_SIZE.key
    val bufferMaxProperty = FORY_SERIALIZER_MAX_BUFFER_SIZE.key

    def newForyInstance(
        conf: SparkConf,
        bufferSize: String = "64k",
        maxBufferSize: String = "64m"): SerializerInstance = {
      val foryConf = conf.clone()
      foryConf.set(bufferSizeProperty, bufferSize)
      foryConf.set(bufferMaxProperty, maxBufferSize)
      // Use COMPATIBLE mode for this test
      foryConf.set("spark.fory.compatible", "COMPATIBLE")
      new ForySerializer(foryConf).newInstance()
    }

    // test default values
    newForyInstance(conf, "64k", "64m")
    // should throw exception with bufferSize out of bound
    val thrown1 = intercept[IllegalArgumentException](newForyInstance(conf, "2048m"))
    assert(thrown1.getMessage.contains(bufferSizeProperty))
    // should throw exception with maxBufferSize out of bound
    val thrown2 = intercept[IllegalArgumentException](
        newForyInstance(conf, maxBufferSize = "2048m"))
    assert(thrown2.getMessage.contains(bufferMaxProperty))
  }

  test("serialization buffer overflow reporting") {
    import org.apache.spark.SparkException
    val bufferMaxProperty = FORY_SERIALIZER_MAX_BUFFER_SIZE.key

    val largeObject = (1 to 1000000).toArray

    val conf = new SparkConf(false)
    conf.set(bufferMaxProperty, "1")
    // Use COMPATIBLE mode for this test
    conf.set("spark.fory.compatible", "COMPATIBLE")

    val ser = new ForySerializer(conf).newInstance()
    val thrown = intercept[SparkException](ser.serialize(largeObject))
    assert(thrown.getMessage.contains(bufferMaxProperty))
  }

  test("Fory with collect") {
    val control = 1 :: 2 :: Nil
    val result = sc.parallelize(control, 2)
      .map(new TestClassWithoutNoArgConstructor(_))
      .collect()
      .map(_.x)
    assert(control === result.toSeq)
  }

  test("Fory with parallelize") {
    val control = 1 :: 2 :: Nil
    val result =
      sc.parallelize(control.map(new TestClassWithoutNoArgConstructor(_))).map(_.x).collect()
    assert(control === result.toSeq)
  }

  test("Fory with parallelize for specialized tuples") {
    assert(sc.parallelize(Seq((1, 11), (2, 22), (3, 33))).count() === 3)
  }

  test("Fory with parallelize for primitive arrays") {
    assert(sc.parallelize(Array(1, 2, 3).toImmutableArraySeq).count() === 3)
  }

  test("Fory with collect for specialized tuples") {
    assert(sc.parallelize(Seq((1, 11), (2, 22), (3, 33))).collect().head === ((1, 11)))
  }
}

object ForySerializerSuite {
  class TestClassWithoutNoArgConstructor(val x: Int) {
    override def hashCode(): Int = x

    override def equals(other: Any): Boolean = other match {
      case c: TestClassWithoutNoArgConstructor => x == c.x
      case _ => false
    }
  }
}