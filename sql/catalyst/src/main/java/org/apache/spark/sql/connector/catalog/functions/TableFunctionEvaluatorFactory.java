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

package org.apache.spark.sql.connector.catalog.functions;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;

/**
 * A serializable factory that produces {@link TableFunctionEvaluator evaluators} for a
 * {@link SupportsTableArgument table-argument function}.
 * <p>
 * A {@link SupportsTableArgument} function returns a factory (not an evaluator) so that only the
 * lightweight, serializable factory crosses the driver-to-executor boundary; the evaluator itself
 * -- which may hold non-serializable state -- is created on the executor via {@link #create()},
 * once per task. This mirrors the {@code PartitionReaderFactory} / {@code PartitionReader} split on
 * the DSv2 read path.
 *
 * @since 4.3.0
 */
@Evolving
public interface TableFunctionEvaluatorFactory extends Serializable {

  /**
   * Creates an evaluator. Called on the executor, once per task.
   */
  TableFunctionEvaluator create();
}
