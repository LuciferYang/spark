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

package test.org.apache.spark.sql;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.network.crypto.AuthMessage;

import java.util.*;
import java.util.stream.Collectors;

public class TestApis {
    // CustomSumMetric
    public static long sumUseStreamApi(long[] input) {
        return Arrays.stream(input).sum();
    }

    // CustomSumMetric
    public static long sumsUseLoopApi(long[] input) {
        long sum = 0L;
        for (long l : input) {
            sum += l;
        }
        return sum;
    }

    // CustomAvgMetric
    public static double avgUseStreamApi(long[] input) {
        if (input.length > 0) {
            return Arrays.stream(input).sum() / (double) input.length;
        } else {
            return 0D;
        }
    }

    // CustomAvgMetric
    public static double avgUseLoopApi(long[] input) {
        if (input.length > 0) {
            long sum = 0L;
            for (long taskMetric : input) {
                sum += taskMetric;
            }
            return ((double) sum) / input.length;
        } else {
            return 0D;
        }
    }

    // V2ExpressionSQLBuilder
    public static String[] mapToStringArrayUseStreamApi(long[] input) {
      return Arrays.stream(input).mapToObj(String::valueOf).toArray(String[]::new);
    }

    // V2ExpressionSQLBuilder
    public static String[] mapToStringArrayUseLoopApi(long[] input) {
        String[] ret = new String[input.length];
        for (int i = 0; i < input.length; i++) {
            ret[i] = String.valueOf(input[i]);
        }
        return ret;
    }

    // V2ExpressionSQLBuilder
    public static List<String> mapToStringListUseStreamApi(long[] input) {
        return Arrays.stream(input).mapToObj(String::valueOf).collect(Collectors.toList());
    }

    // V2ExpressionSQLBuilder
    public static List<String> mapToStringListUseLoopApi(long[] input) {
        List<String> list = new ArrayList<>();
        for (long l : input) {
            String s = String.valueOf(l);
            list.add(s);
        }
        return list;
    }

    public static void foreachOrderUseStreamApi(String[] input) {
        Arrays.stream(input).forEachOrdered(String::length);
    }

    public static void foreachOrderUseLoopApi(String[] input) {
        for (String s : input) {
            s.length();
        }
    }

    public static int mapToLengthAndSumUseStreamApi(String[] input) {
      return Arrays.stream(input).mapToInt(String::length).sum();
    }

    public static int mapToLengthAndSumUseLoopApi(String[] input) {
        int sum = 0;
        for (String s : input) {
            int length = s.length();
            sum += length;
        }
        return sum;
    }

    public static boolean anyMatchUseStreamApi(long[] input, long target) {
      return Arrays.stream(input).anyMatch(l  -> l > target);
    }

    public static boolean allMatchUseStreamApi(long[] input, long target) {
        return Arrays.stream(input).allMatch(l  -> l > target);
    }

    public static boolean allMatchUseLoopApi(long[] input, long target) {
        for (long l : input) {
            if (l <= target) {
                return false;
            }
        }
        return true;
    }

    public static boolean anyMatchUseLoopApi(long[] input, long target) {
        for (long l : input) {
            if (l > target) {
                return true;
            }
        }
        return false;
    }

    public static TestObj[] objs(int length, int size, int range) {
        TestObj[] objects = new TestObj[length];
        for (int i = 0; i < length; i++) {
            objects[i] = new TestObj(size, range);
        }
        return objects;
    }

    public static TestValue[] distinctUseStreamApi(TestObj[] input) {
      return Arrays.stream(input).map(s -> s.values)
        .flatMap(Arrays::stream).distinct().toArray(TestValue[]::new);
    }

    public static TestValue[] distinctUseLoopApi(TestObj[] input) {
        List<TestValue> list = new ArrayList<>();
        Set<TestValue> uniqueValues = new HashSet<>();
        for (TestObj s : input) {
            TestValue[] values = s.values;
            for (TestValue testValue : values) {
                if (uniqueValues.add(testValue)) {
                    list.add(testValue);
                }
            }
        }
        return list.toArray(new TestValue[0]);
    }

    public static TestValue[] distinctUseLinkedHashSet(TestObj[] input) {
        Set<TestValue> set = new LinkedHashSet<>();
        for (TestObj s : input) {
            TestValue[] values = s.values;
            for (TestValue testValue : values) {
                set.add(testValue);
            }
        }
        return set.toArray(new TestValue[0]);
    }


    public static class TestObj {
        TestValue[] values;

        public TestObj(int size, int range) {
            values = new TestValue[size];
            for (int i = 0; i < values.length; i++) {
                values[i] = new TestValue(RandomUtils.nextInt(0, range));
            }
        }
    }

    public static class TestValue {
        private int value;

        public TestValue(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestValue testValue = (TestValue) o;
            return value == testValue.value;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value);
        }
    }

    public static int authMessagesLengthUseStreamApi(AuthMessage[] encryptedPublicKeys) {
      return Arrays.stream(encryptedPublicKeys).mapToInt(k -> k.encodedLength()).sum();
    }

    public static int authMessagesLengthUseLoopApi(AuthMessage[] encryptedPublicKeys) {
      int initialCapacity = 0;
      for (AuthMessage k : encryptedPublicKeys) {
        initialCapacity += k.encodedLength();
      }
      return initialCapacity;
    }

    public static  byte[] encodeAuthMessagesUseStreamApi(int length, AuthMessage[] encryptedPublicKeys) {
        ByteBuf transcript = Unpooled.buffer(length);
        Arrays.stream(encryptedPublicKeys).forEachOrdered(k -> k.encode(transcript));
        return transcript.array();
    }

    public static byte[] encodeAuthMessagesUseLoopApi(int length, AuthMessage[] encryptedPublicKeys) {
        ByteBuf transcript = Unpooled.buffer(length);
        for (AuthMessage k : encryptedPublicKeys) {
            k.encode(transcript);
        }
        return transcript.array();
    }
}
