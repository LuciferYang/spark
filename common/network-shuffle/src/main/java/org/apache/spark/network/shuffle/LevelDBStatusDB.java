package org.apache.spark.network.shuffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.parseDbAppExecKey;

public class LevelDBStatusDB implements LocalStatusDB {

    private static final Logger logger = LoggerFactory.getLogger(LevelDBStatusDB.class);

    private static final ObjectMapper mapper = new ObjectMapper();

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
    public ConcurrentMap<ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo> reloadRegisteredExecutors(
        String appKeyPrefix) throws IOException {
        ConcurrentMap<ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo> registeredExecutors =
          Maps.newConcurrentMap();
        if (backend != null) {
            DBIterator itr = backend.iterator();
            itr.seek(appKeyPrefix.getBytes(StandardCharsets.UTF_8));
            while (itr.hasNext()) {
                Map.Entry<byte[], byte[]> e = itr.next();
                String key = new String(e.getKey(), StandardCharsets.UTF_8);
                if (!key.startsWith(appKeyPrefix)) {
                    break;
                }
                ExternalShuffleBlockResolver.AppExecId id = parseDbAppExecKey(key);
                logger.info("Reloading registered executors: " +  id.toString());
                ExecutorShuffleInfo shuffleInfo = mapper.readValue(e.getValue(), ExecutorShuffleInfo.class);
                registeredExecutors.put(id, shuffleInfo);
            }
        }
        return registeredExecutors;
    }

    @Override
    public Object backend() {
        return backend;
    }
}
