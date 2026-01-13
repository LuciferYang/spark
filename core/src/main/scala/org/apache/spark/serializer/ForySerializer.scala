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

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.fory.{Fory => ForyLib}
import org.apache.fory.config.{CompatibleMode, Language}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Fory
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream}

/**
 * :: DeveloperApi ::
 * A Spark serializer that uses the Apache Fory serialization library.
 *
 * Apache Fory is a blazing fast multi-language serialization framework powered by JIT
 * and zero-copy, providing up to 170x performance improvement over traditional serializers.
 *
 * Key features:
 * - High performance: 20-170x faster than traditional serializers
 * - Multi-language support (Java, Python, C++, Golang, JavaScript, Rust, Scala, Kotlin, TypeScript)
 * - Zero-copy serialization
 * - JIT-based optimization
 * - Automatic schema evolution support
 *
 * @note This serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
@DeveloperApi
class ForySerializer(conf: SparkConf)
  extends Serializer
  with Logging
  with Serializable {

  // Configuration parameters using official config keys
  private[serializer] val bufferSizeKb = conf.get(Fory.FORY_SERIALIZER_BUFFER_SIZE).toInt
  private[serializer] val bufferSize = ByteUnit.KiB.toBytes(bufferSizeKb).toInt

  private[serializer] val maxBufferSizeMb = conf.get(Fory.FORY_SERIALIZER_MAX_BUFFER_SIZE).toInt
  private[serializer] val maxBufferSize = ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt

  private[serializer] val referenceTracking = conf.get(Fory.FORY_REFERENCE_TRACKING)
  private[serializer] val registrationRequired = false // Disable class registration for Scala specialization
  private[serializer] val useUnsafe = conf.get(Fory.FORY_USE_UNSAFE)
  private[serializer] val compatibleMode = CompatibleMode.valueOf(conf.get(Fory.FORY_COMPATIBLE))

  // Thread-local Fory instances for thread safety
  private[spark] lazy val foryThreadLocal: ThreadLocal[ForyLib] = new ThreadLocal[ForyLib] {
    override def initialValue(): ForyLib = {
      ForyLib.builder()
        .withRefTracking(referenceTracking)
        .requireClassRegistration(false) // Disable class registration to handle Scala specialization
        .withLanguage(Language.JAVA)
        .withCompatibleMode(CompatibleMode.COMPATIBLE) // Force compatible mode for schema evolution
        .build()
    }
  }

  override def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    super.setDefaultClassLoader(classLoader)
    // Clear thread-local instances so they get recreated with the new classloader
    foryThreadLocal.remove()
    this
  }

  override def newInstance(): SerializerInstance = {
    new ForySerializerInstance(this)
  }

  /**
   * :: Private ::
   * Fory supports relocation of serialized objects as it uses stateless serialization.
   */
  @DeveloperApi
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private[spark] class ForySerializationStream(
    serInstance: ForySerializerInstance,
    outStream: OutputStream) extends SerializationStream {

  private[this] val fory = serInstance.borrowFory()
  private[this] val outputBuffer = new ByteArrayOutputStream()

  // Access parent serializer configuration
  private[this] val bufferSize = serInstance.fs.bufferSize
  private[this] val maxBufferSize = serInstance.fs.maxBufferSize
  private[this] val useUnsafe = serInstance.fs.useUnsafe

  // Make releaseFory accessible to serialization stream
  private[this] val releaseFory: ForyLib => Unit = serInstance.releaseFory

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      val serialized = fory.serialize(t.asInstanceOf[AnyRef])

      // Write length prefix for proper stream deserialization
      val length = serialized.length
      outputBuffer.write((length >> 24) & 0xFF)
      outputBuffer.write((length >> 16) & 0xFF)
      outputBuffer.write((length >> 8) & 0xFF)
      outputBuffer.write(length & 0xFF)
      outputBuffer.write(serialized)

      // Optional: Use buffer size based flushing
      if (outputBuffer.size() >= maxBufferSize) {
        flush()
      }

      this
    } catch {
      case e: Exception =>
        throw new IOException(s"Fory serialization failed: ${e.getMessage}", e)
    }
  }

  override def flush(): Unit = {
    if (outputBuffer != null && outputBuffer.size() > 0) {
      outputBuffer.writeTo(outStream)
      outStream.flush()
      outputBuffer.reset()
    }
  }

  override def close(): Unit = {
    if (outputBuffer != null) {
      try {
        flush()
      } finally {
        outputBuffer.close()
        // Don't close outStream here as it's managed by the caller
        releaseFory(fory)
      }
    }
  }
}

private[spark] class ForyDeserializationStream(
    serInstance: ForySerializerInstance,
    inStream: InputStream) extends DeserializationStream {

  private[this] val fory = serInstance.borrowFory()
  // Reusable buffer for reading operations
  private[this] val inputBuffer = new Array[Byte](8192)

  // Access parent serializer configuration
  private[this] val bufferSize = serInstance.fs.bufferSize

  // Make releaseFory accessible to deserialization stream
  private[this] val releaseFory: ForyLib => Unit = serInstance.releaseFory

  override def readObject[T: ClassTag](): T = {
    try {
      // Read length prefix
      val lengthBytes = inputBuffer.take(4)
      var bytesRead = 0
      while (bytesRead < 4) {
        val read = inStream.read(lengthBytes, bytesRead, 4 - bytesRead)
        if (read == -1) {
          throw new EOFException
        }
        bytesRead += read
      }

      val length =
        (lengthBytes(0) << 24) | (lengthBytes(1) << 16) | (lengthBytes(2) << 8) | lengthBytes(3)

      // Use bufferSize for larger read operations when possible
      val serializedBytes = if (length > bufferSize) {
        new Array[Byte](length)
      } else {
        if (inputBuffer.length < length) {
          // Expand buffer if needed
          new Array[Byte](length)
        } else {
          inputBuffer
        }
      }

      bytesRead = 0
      while (bytesRead < length) {
        val toRead = Math.min(inputBuffer.length, length - bytesRead)
        val read = inStream.read(serializedBytes, bytesRead, toRead)
        if (read == -1) {
          throw new EOFException
        }
        bytesRead += read
      }

      // Use explicit class information for deserialization
      try {
        val clazz = implicitly[ClassTag[T]].runtimeClass
        fory.deserialize(serializedBytes, clazz).asInstanceOf[T]
      } catch {
        case e: Exception =>
          // Fallback to simple deserialize if explicit class fails
          try {
            fory.deserialize(serializedBytes).asInstanceOf[T]
          } catch {
            case ex: Exception =>
              throw new IOException(s"All deserialization attempts failed: ${e.getMessage}, fallback: ${ex.getMessage}", e)
          }
      }
    } catch {
      case e: EOFException => throw e
      case e: Exception =>
        throw new IOException(s"Fory deserialization failed: ${e.getMessage}", e)
    }
  }

  override def close(): Unit = {
    releaseFory(fory)
  }
}

private[spark] class ForySerializerInstance(
    val fs: ForySerializer) extends SerializerInstance {

  private[this] var cachedFory: ForyLib = _

  /**
   * Borrows a Fory instance. Tries to re-use a cached instance if available.
   */
  private[serializer] def borrowFory(): ForyLib = {
    if (cachedFory != null) {
      val instance = cachedFory
      cachedFory = null
      instance
    } else {
      // Directly access the parent serializer's thread-local
      fs.foryThreadLocal.get()
    }
  }

  /**
   * Releases a borrowed Fory instance for re-use.
   */
  private[serializer] def releaseFory(fory: ForyLib): Unit = {
    if (cachedFory == null) {
      cachedFory = fory
    }
  }

  // Removed unused method

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    try {
      val out = serializeStream(bos)
      out.writeObject(t)
      out.close()
      bos.close()
      bos.toByteBuffer
    } catch {
      case e: Exception =>
        bos.close()
        throw e
    }
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    // For Fory, class loading is typically handled internally
    deserialize(bytes)
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new ForySerializationStream(this, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new ForyDeserializationStream(this, s)
  }
}

private[spark] object ForySerializer extends Logging {

  // Configuration key references for internal use
  val FORY_SERIALIZER_BUFFER_SIZE = Fory.FORY_SERIALIZER_BUFFER_SIZE
  val FORY_SERIALIZER_MAX_BUFFER_SIZE = Fory.FORY_SERIALIZER_MAX_BUFFER_SIZE
  val FORY_REFERENCE_TRACKING = Fory.FORY_REFERENCE_TRACKING
  val FORY_REGISTRATION_REQUIRED = Fory.FORY_REGISTRATION_REQUIRED
  val FORY_USE_UNSAFE = Fory.FORY_USE_UNSAFE
}