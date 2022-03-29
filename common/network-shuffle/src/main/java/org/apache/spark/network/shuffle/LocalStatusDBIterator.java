package org.apache.spark.network.shuffle;

public interface LocalStatusDBIterator {

    void seek(byte[] key);

    boolean hasNext();
}
