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

package org.apache.spark.network.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JacksonMapper {

    protected static String toJson(ObjectMapper mapper, Object object) throws IOException {
      return mapper.writeValueAsString(object);
    }

    protected static byte[] toJsonByteArray(ObjectMapper mapper, Object object) throws IOException {
        return mapper.writeValueAsBytes(object);
    }

    protected static <T> T fromJson(ObjectMapper mapper, String jsonString, Class<T> clazz) throws IOException {
        return mapper.readValue(jsonString, clazz);
    }

    protected static <T> T fromJson(ObjectMapper mapper, byte[] byteArray, Class<T> clazz) throws IOException {
        return mapper.readValue(byteArray, clazz);
    }

    protected static <T> T fromJson(ObjectMapper mapper, String content, TypeReference<T> valueTypeRef) throws JsonProcessingException {
        return mapper.readValue(content, valueTypeRef);
    }

    public static class Default {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        public static String toJson(Object object) throws IOException {
            return JacksonMapper.toJson(MAPPER, object);
        }

        public static byte[] toJsonByteArray(Object object) throws IOException {
            return JacksonMapper.toJsonByteArray(MAPPER, object);
        }

        public static <T> T fromJson(String jsonString, Class<T> clazz) throws IOException {
            return JacksonMapper.fromJson(MAPPER, jsonString, clazz);
        }

        public static <T> T fromJson(byte[] byteArray, Class<T> clazz) throws IOException {
            return JacksonMapper.fromJson(MAPPER, byteArray, clazz);
        }

        public static <T> T fromJson(String content, TypeReference<T> valueTypeRef) throws JsonProcessingException {
            return JacksonMapper.fromJson(MAPPER, content, valueTypeRef);
        }
    }
}
