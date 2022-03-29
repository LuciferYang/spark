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

package org.apache.spark.network.shuffle;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;

public class RocksDBStatusDB implements LocalStatusDB {

    private RocksDB backend;

    public RocksDBStatusDB(RocksDB backend) {
        this.backend = backend;
    }

    @Override
    public void put(byte[] key, byte[] value) throws RuntimeException {
        try {
            backend.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(byte[] key) throws RuntimeException {
        try {
            backend.delete(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        backend.close();
    }

    @Override
    public LocalStatusDBIterator iterator() {
        RocksIterator rocksIterator = backend.newIterator();
        return null;
    }
}
