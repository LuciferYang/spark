package org.apache.spark.network.shuffle;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.util.Iterator;

public class LevelDBStatusDB implements LocalStatusDB {

    private DB backend;

    public LevelDBStatusDB(DB backend) {
        this.backend = backend;
    }

    @Override
    public void put(byte[] key, byte[] value) throws RuntimeException {
        backend.put(key, value);
    }

    @Override
    public void delete(byte[] key) throws RuntimeException {
        backend.delete(key);
    }

    @Override
    public void close() throws IOException {
        backend.close();
    }

    @Override
    public Iterator iterator() {
        DBIterator iterator = backend.iterator();
        return null;
    }
}
