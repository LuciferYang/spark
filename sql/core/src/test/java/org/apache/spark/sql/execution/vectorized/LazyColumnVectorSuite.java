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

package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class LazyColumnVectorSuite {

  @Test
  public void testLazyLoading() {
    OnHeapColumnVector underlying = new OnHeapColumnVector(10, DataTypes.IntegerType);
    LazyColumnVector lazy = new LazyColumnVector(underlying);
    AtomicBoolean loaded = new AtomicBoolean(false);

    lazy.setLoadTask(() -> {
      loaded.set(true);
      underlying.putInt(0, 123);
    });

    Assert.assertFalse("Should not be loaded initially", loaded.get());

    // Calling put should not trigger load
    lazy.putInt(1, 456);
    Assert.assertFalse("Put should not trigger load", loaded.get());

    // Calling get should trigger load
    int val = lazy.getInt(0);
    Assert.assertTrue("Get should trigger load", loaded.get());
    Assert.assertEquals(123, val);
    Assert.assertEquals(456, lazy.getInt(1)); // The value we put manually should also be there

    // Reset should clear loaded status
    lazy.reset();
    loaded.set(false);
    lazy.setLoadTask(() -> {
        loaded.set(true);
        underlying.putInt(0, 789);
    });
    
    Assert.assertFalse("Should not be loaded after reset", loaded.get());
    
    val = lazy.getInt(0);
    Assert.assertTrue("Get after reset should trigger load", loaded.get());
    Assert.assertEquals(789, val);
    
    lazy.close();
  }

  @Test
  public void testNestedLoading() {
    StructType structType = new StructType()
        .add("a", DataTypes.IntegerType)
        .add("b", DataTypes.IntegerType);
    
    OnHeapColumnVector underlying = new OnHeapColumnVector(10, structType);
    LazyColumnVector lazy = new LazyColumnVector(underlying);
    AtomicBoolean loaded = new AtomicBoolean(false);

    lazy.setLoadTask(() -> {
      loaded.set(true);
      underlying.getChild(0).putInt(0, 100);
      underlying.getChild(1).putInt(0, 200);
    });

    Assert.assertFalse("Should not be loaded initially", loaded.get());

    // Accessing child should trigger load
    WritableColumnVector child0 = lazy.getChild(0);
    Assert.assertTrue("GetChild should trigger load", loaded.get());
    
    Assert.assertEquals(100, child0.getInt(0));
    
    lazy.close();
  }
}
