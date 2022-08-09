package org.apache.spark.sql;

import org.apache.commons.lang3.RandomUtils;

public class TestObject {

    private final Integer[] children;

    public TestObject(int length) {
        children = new Integer[length];
        for (int i = 0; i < length; i++) {
            children[i] = RandomUtils.nextInt(0, length + 1000);
        }
    }

    public Integer[] getChildren() {
        return children;
    }
}
