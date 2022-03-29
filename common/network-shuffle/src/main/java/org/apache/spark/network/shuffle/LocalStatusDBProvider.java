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
import org.apache.spark.network.util.LevelDBProvider;
import org.apache.spark.network.util.RocksDBProvider;
import org.apache.spark.network.util.StoreVersion;
import org.iq80.leveldb.DB;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;

public class LocalStatusDBProvider {

    public static LocalStatusDB initDB(String dbImpl, File dbFile, StoreVersion version, ObjectMapper mapper) throws
            IOException {
        if ("ldb".equals(dbImpl)) {
            DB levelDB = LevelDBProvider.initLevelDB(dbFile, version, mapper);
            return levelDB != null ? new LevelDBStatusDB(levelDB) : null;
        }

        if ("rdb".equals(dbImpl)) {
            RocksDB rocksDB = RocksDBProvider.initRockDB(dbFile, version, mapper);
            return rocksDB != null ? new RocksDBStatusDB(rocksDB) : null;
        }

        return null;
    }
}
