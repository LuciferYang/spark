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

package org.apache.spark.sql.execution.datasources.parquet;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.PrimitiveIterator;

import org.apache.parquet.column.ColumnDescriptor;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * Test-only reflective bridge to the package-private {@link ParquetReadState}. Benchmarks run via
 * spark-submit load test jars through MutableURLClassLoader, while main sql-core lives on the app
 * classloader — this creates distinct runtime packages and blocks direct package-private access.
 * Reflection with setAccessible sidesteps the check without widening production visibility.
 */
public final class ParquetReadStateTestAccess {

  private static final Constructor<?> CTOR;
  private static final Method RESET_FOR_NEW_BATCH;
  private static final Method RESET_FOR_NEW_PAGE;
  private static final Method READ_BATCH;

  static {
    try {
      Class<?> stateCls = Class.forName(
          "org.apache.spark.sql.execution.datasources.parquet.ParquetReadState");
      CTOR = stateCls.getDeclaredConstructor(
          ColumnDescriptor.class, boolean.class, PrimitiveIterator.OfLong.class);
      CTOR.setAccessible(true);
      RESET_FOR_NEW_BATCH = stateCls.getDeclaredMethod("resetForNewBatch", int.class);
      RESET_FOR_NEW_BATCH.setAccessible(true);
      RESET_FOR_NEW_PAGE =
          stateCls.getDeclaredMethod("resetForNewPage", int.class, long.class);
      RESET_FOR_NEW_PAGE.setAccessible(true);
      // readBatch is public; locate it without a class literal on ParquetReadState. Match on
      // name, arity, and the first-parameter type (the state class) to guard against future
      // overload ambiguity should a second 5-arg `readBatch` ever be added.
      Method found = null;
      for (Method m : VectorizedRleValuesReader.class.getMethods()) {
        if (m.getName().equals("readBatch")
            && m.getParameterCount() == 5
            && m.getParameterTypes()[0] == stateCls) {
          found = m;
          break;
        }
      }
      if (found == null) {
        throw new NoSuchMethodException("VectorizedRleValuesReader.readBatch/5");
      }
      READ_BATCH = found;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to initialize ParquetReadStateTestAccess", e);
    }
  }

  private ParquetReadStateTestAccess() {}

  public static Object newState(ColumnDescriptor descriptor, boolean isRequired) {
    return newState(descriptor, isRequired, null);
  }

  public static Object newState(
      ColumnDescriptor descriptor,
      boolean isRequired,
      PrimitiveIterator.OfLong rowIndexes) {
    try {
      return CTOR.newInstance(descriptor, isRequired, rowIndexes);
    } catch (ReflectiveOperationException e) {
      throw rethrow(e);
    }
  }

  public static void resetForNewBatch(Object state, int batchSize) {
    try {
      RESET_FOR_NEW_BATCH.invoke(state, batchSize);
    } catch (ReflectiveOperationException e) {
      throw rethrow(e);
    }
  }

  public static void resetForNewPage(Object state, int totalValuesInPage, long pageFirstRowIndex) {
    try {
      RESET_FOR_NEW_PAGE.invoke(state, totalValuesInPage, pageFirstRowIndex);
    } catch (ReflectiveOperationException e) {
      throw rethrow(e);
    }
  }

  public static void readBatch(
      VectorizedRleValuesReader reader,
      Object state,
      WritableColumnVector values,
      WritableColumnVector defLevels,
      VectorizedValuesReader valueReader,
      ParquetVectorUpdater updater) {
    try {
      READ_BATCH.invoke(reader, state, values, defLevels, valueReader, updater);
    } catch (ReflectiveOperationException e) {
      throw rethrow(e);
    }
  }

  /**
   * Unwraps {@link InvocationTargetException} so assertion failures and exceptions thrown by
   * the target method surface with their real stack trace. Other reflective errors (access,
   * missing method) are wrapped in a {@link RuntimeException} unchanged.
   */
  private static RuntimeException rethrow(ReflectiveOperationException e) {
    Throwable cause = (e instanceof InvocationTargetException) ? e.getCause() : e;
    if (cause instanceof RuntimeException re) {
      throw re;
    }
    if (cause instanceof Error er) {
      throw er;
    }
    throw new RuntimeException(cause);
  }
}
