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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentMap;

import static org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.parseDbAppExecKey;


public class RocksDBStatusDB implements LocalStatusDB {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBStatusDB.class);

    private static final ObjectMapper mapper = new ObjectMapper();

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
    public ConcurrentMap<ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo> reloadRegisteredExecutors(
        String appKeyPrefix) throws IOException {
        ConcurrentMap<ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo> registeredExecutors =
                Maps.newConcurrentMap();
        if (backend != null) {
            RocksIterator itr = backend.newIterator();
            itr.seek(appKeyPrefix.getBytes(StandardCharsets.UTF_8));
            while (itr.isValid()) {
                String key = new String(itr.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(appKeyPrefix)) {
                    break;
                }
                ExternalShuffleBlockResolver.AppExecId id = parseDbAppExecKey(key);
                logger.info("Reloading registered executors: " + id.toString());
                ExecutorShuffleInfo shuffleInfo = mapper.readValue(itr.value(), ExecutorShuffleInfo.class);
                registeredExecutors.put(id, shuffleInfo);
                itr.next();
            }
        }
        return registeredExecutors;
    }

    @Override
    public Object backend() {
        return backend;
    }
}
