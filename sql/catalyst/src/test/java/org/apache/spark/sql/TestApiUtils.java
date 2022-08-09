package org.apache.spark.sql;

import java.util.*;

public class TestApiUtils {
    public static Integer[] streamApi(TestObject[] objects) {
      return Arrays.stream(objects).map(TestObject::getChildren)
        .flatMap(Arrays::stream).distinct().toArray(Integer[]::new);
    }

    public static Integer[] loopApi(TestObject[] objects) {
        List<Integer> list = new ArrayList<>();
        Set<Integer> uniqueValues = new HashSet<>();
        for (TestObject object : objects) {
            Integer[] children = object.getChildren();
            for (Integer integer : children) {
                if (uniqueValues.add(integer)) {
                    list.add(integer);
                }
            }
        }
        return list.toArray(new Integer[0]);
    }
}
