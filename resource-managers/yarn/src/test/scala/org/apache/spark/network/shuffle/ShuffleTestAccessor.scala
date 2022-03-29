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
package org.apache.spark.network.shuffle

import java.io.File
import java.util.concurrent.ConcurrentMap

import org.apache.hadoop.yarn.api.records.ApplicationId

import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo

/**
 * just a cheat to get package-visible members in tests
 */
object ShuffleTestAccessor {

  def getBlockResolver(handler: ExternalBlockHandler): ExternalShuffleBlockResolver = {
    handler.blockManager
  }

  def getExecutorInfo(
      appId: ApplicationId,
      execId: String,
      resolver: ExternalShuffleBlockResolver
  ): Option[ExecutorShuffleInfo] = {
    val id = new AppExecId(appId.toString, execId)
    Option(resolver.executors.get(id))
  }

  def registeredExecutorFile(resolver: ExternalShuffleBlockResolver): File = {
    resolver.registeredExecutorFile
  }

  def shuffleServiceLocalDB(resolver: ExternalShuffleBlockResolver): LocalStatusDB = {
    resolver.db
  }

  def reloadRegisteredExecutors(
    dbImpl: String,
    file: File): ConcurrentMap[ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo] = {
    import com.fasterxml.jackson.databind.ObjectMapper
    import org.apache.spark.network.util.StoreVersion
    val version = new StoreVersion(1, 0)
    val mapper = new ObjectMapper()
    val db = LocalStatusDBProvider.initDB(dbImpl, file, version, mapper)
    val result = ExternalShuffleBlockResolver.reloadRegisteredExecutors(db)
    db.close()
    result
  }

  def reloadRegisteredExecutors(db: LocalStatusDB):
      ConcurrentMap[ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo] = {
    ExternalShuffleBlockResolver.reloadRegisteredExecutors(db)
  }
}
