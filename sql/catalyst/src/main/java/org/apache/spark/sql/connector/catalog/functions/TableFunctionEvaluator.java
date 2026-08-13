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

import java.util.Iterator;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * An executor-side evaluator for a {@link SupportsTableArgument table-argument function}, created
 * by a {@link TableFunctionEvaluatorFactory}. It transforms the rows of a single input partition
 * into output rows.
 * <p>
 * An input partition is one PARTITION BY group: Spark distributes and sorts the rows of the TABLE
 * argument according to the function's {@link SupportsTableArgument#requiredDistribution()} and
 * {@link SupportsTableArgument#requiredOrdering()} (combined with any call-site PARTITION BY / WITH
 * SINGLE PARTITION / ORDER BY clause), then invokes {@link #eval(Iterator)} exactly once per group,
 * passing the group's rows in the required order. The union of all per-group outputs is the
 * function's result.
 * <p>
 * The evaluator is not required to be thread-safe; Spark uses each instance from a single task.
 *
 * @since 4.3.0
 */
@Evolving
public interface TableFunctionEvaluator {

  /**
   * Transforms the rows of a single input partition into output rows.
   * <p>
   * The rows in {@code partition} share the same values for the required distribution's clustering
   * expressions and arrive in the required order. The returned rows must conform to the function's
   * {@link BoundTableFunction#resultSchema() result schema}. The input iterator must be consumed
   * lazily and not retained after the returned iterator is exhausted.
   *
   * @param partition the rows of a single input partition (one PARTITION BY group)
   * @return the output rows produced for this partition
   */
  Iterator<InternalRow> eval(Iterator<InternalRow> partition);
}
