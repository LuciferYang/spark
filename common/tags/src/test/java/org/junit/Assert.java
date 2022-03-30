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

package org.junit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

public class Assert {

    public static void assertTrue(String message, boolean condition) {
      Assertions.assertTrue(condition, message);
    }

    public static void assertTrue(boolean condition) {
      Assertions.assertTrue(condition);
    }

    public static void assertFalse(String message, boolean condition) {
      Assertions.assertFalse(condition, message);
    }

    public static void assertFalse(boolean condition) {
      Assertions.assertFalse(condition);
    }

    public static void fail(String message) {
      Assertions.fail(message);
    }

    public static void fail() {
        Assertions.fail();
    }

    public static void assertEquals(String message, Object expected, Object actual) {
      Assertions.assertEquals(expected, actual, message);
    }

    public static void assertEquals(Object expected, Object actual) {
        Assertions.assertEquals(expected, actual);
    }

    public static void assertNotEquals(String message, Object unexpected, Object actual) {
      Assertions.assertNotEquals(unexpected, actual, message);
    }

    public static void assertNotEquals(Object unexpected, Object actual) {
        Assertions.assertNotEquals(unexpected, actual);
    }

    public static void assertNotEquals(String message, long unexpected, long actual) {
      Assertions.assertNotEquals(unexpected, actual, message);
    }

    public static void assertNotEquals(long unexpected, long actual) {
      Assertions.assertNotEquals(unexpected, actual);
    }

    public static void assertNotEquals(String message, double unexpected, double actual, double delta) {
      Assertions.assertNotEquals(unexpected, actual, delta, message);
    }

    public static void assertNotEquals(double unexpected, double actual, double delta) {
      Assertions.assertNotEquals(unexpected, actual, delta);
    }

    public static void assertNotEquals(float unexpected, float actual, float delta) {
      Assertions.assertNotEquals(unexpected, actual, delta);
    }

    public static void assertArrayEquals(String message, Object[] expecteds, Object[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals, message);
    }

    public static void assertArrayEquals(Object[] expecteds, Object[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals);
    }

    public static void assertArrayEquals(String message, boolean[] expecteds, boolean[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals, message);
    }

    public static void assertArrayEquals(boolean[] expecteds, boolean[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals);
    }

    public static void assertArrayEquals(String message, byte[] expecteds, byte[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals, message);
    }

    public static void assertArrayEquals(byte[] expecteds, byte[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals);
    }

    public static void assertArrayEquals(String message, char[] expecteds, char[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals, message);
    }

    public static void assertArrayEquals(char[] expecteds, char[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals);
    }

    public static void assertArrayEquals(String message, short[] expecteds, short[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals, message);
    }

    public static void assertArrayEquals(short[] expecteds, short[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals);
    }

    public static void assertArrayEquals(String message, int[] expecteds, int[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals, message);
    }

    public static void assertArrayEquals(int[] expecteds, int[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals);
    }

    public static void assertArrayEquals(String message, long[] expecteds, long[] actuals) {
      Assertions.assertArrayEquals(expecteds, actuals, message);
    }

    public static void assertArrayEquals(long[] expecteds, long[] actuals) {
        Assertions.assertArrayEquals(expecteds, actuals);
    }

    public static void assertArrayEquals(String message, double[] expecteds, double[] actuals, double delta) {
      Assertions.assertArrayEquals(expecteds, actuals, delta, message);
    }

    public static void assertArrayEquals(double[] expecteds, double[] actuals, double delta) {
        Assertions.assertArrayEquals(expecteds, actuals, delta);
    }

    public static void assertArrayEquals(String message, float[] expecteds, float[] actuals, float delta) {
        Assertions.assertArrayEquals(expecteds, actuals, delta, message);
    }

    public static void assertArrayEquals(float[] expecteds, float[] actuals, float delta) {
      Assertions.assertArrayEquals(expecteds, actuals, delta);
    }

    public static void assertEquals(String message, double expected, double actual, double delta) {
      Assertions.assertEquals(expected, actual, delta, message);
    }

    public static void assertEquals(String message, float expected, float actual, float delta) {
      Assertions.assertEquals(expected, actual, delta, message);
    }

    public static void assertNotEquals(String message, float unexpected, float actual, float delta) {
      Assertions.assertNotEquals(unexpected, actual, delta, message);
    }

    public static void assertEquals(long expected, long actual) {
      Assertions.assertEquals(expected, actual);
    }

    public static void assertEquals(String message, long expected, long actual) {
      Assertions.assertEquals(expected, actual, message);
    }

    public static void assertEquals(double expected, double actual, double delta) {
      Assertions.assertEquals(expected, actual, delta);
    }

    public static void assertEquals(float expected, float actual, float delta) {
      Assertions.assertEquals(expected, actual, delta);
    }

    public static void assertNotNull(String message, Object object) {
      Assertions.assertNotNull(object, message);
    }

    public static void assertNotNull(Object object) {
      Assertions.assertNotNull(object);
    }

    public static void assertNull(String message, Object object) {
      Assertions.assertNull(object, message);
    }

    public static void assertNull(Object object) {
      Assertions.assertNull(object);
    }

    public static void assertSame(String message, Object expected, Object actual) {
      Assertions.assertSame(expected, actual, message);
    }

    public static void assertSame(Object expected, Object actual) {
      Assertions.assertSame(expected, actual);
    }

    public static void assertNotSame(String message, Object unexpected, Object actual) {
        Assertions.assertNotSame(unexpected, actual, message);
    }

    public static void assertNotSame(Object unexpected, Object actual) {
        Assertions.assertNotSame(unexpected, actual);
    }

    public static <T extends Throwable> T assertThrows(
        Class<T> expectedThrowable, Executable runnable) {
      return assertThrows(null, expectedThrowable, runnable);
    }

    public static <T extends Throwable> T assertThrows(
        String message, Class<T> expectedThrowable, Executable runnable) {
      return Assertions.assertThrows(expectedThrowable, runnable, message);
    }
}
