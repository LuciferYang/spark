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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestApis {
    // CustomSumMetric
    public static long sumUseStreamApi(long[] input) {
        return Arrays.stream(input).sum();
    }

    // CustomSumMetric
    public static long sumsUseLoop(long[] input) {
        long sum = 0L;
        for (long taskMetric : input) {
            sum += taskMetric;
        }
        return sum;
    }

    // CustomAvgMetric
    public static double avgUseStreamApi(long[] input) {
        if (input.length > 0) {
            return ((double)Arrays.stream(input).sum()) / input.length;
        } else {
            return 0D;
        }
    }

    // CustomAvgMetric
    public static double avgUseLoop(long[] input) {
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
        List<String> list = new ArrayList<>();
        for (long l : input) {
            String s = String.valueOf(l);
            list.add(s);
        }
        return list.toArray(new String[0]);
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

    // V2ExpressionSQLBuilder
    public static String joinStreamApiNoPreSuffix(String[] input) {
        return "(" + Arrays.stream(input).collect(Collectors.joining(", ")) + ")";
    }

    // V2ExpressionSQLBuilder
    public static String joinStreamApiWithPreSuffix(String[] input) {
        return Arrays.stream(input).collect(Collectors.joining(", ", "(", ")"));
    }

    // V2ExpressionSQLBuilder
    public static String stringJoinApi(String[] input) {
        return "(" + String.join(", ", input) + ")";
    }
}
