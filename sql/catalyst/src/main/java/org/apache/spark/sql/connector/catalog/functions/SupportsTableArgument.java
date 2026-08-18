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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;

/**
 * A mixin for a {@link BoundTableFunction} that consumes a TABLE argument (a relation) and
 * transforms its rows.
 * <p>
 * A table-argument function declares how its input relation must be distributed and ordered before
 * evaluation, and provides a serializable {@link TableFunctionEvaluatorFactory factory} that Spark
 * ships to executors to run the transform once per input partition.
 * <p>
 * The function-declared {@link #requiredDistribution()} and {@link #requiredOrdering()} are the
 * connector analog of a call-site {@code PARTITION BY} / {@code WITH SINGLE PARTITION} /
 * {@code ORDER BY} clause. Spark satisfies them by inserting the corresponding repartition and sort
 * into the plan. When the call site ALSO specifies partitioning, Spark validates that the two are
 * compatible.
 *
 * @since 4.3.0
 */
@Evolving
public interface SupportsTableArgument extends BoundTableFunction {

  /**
   * Returns the distribution this function requires of its input relation.
   * <p>
   * A {@link org.apache.spark.sql.connector.distributions.ClusteredDistribution} requests that
   * rows sharing the clustering expressions be co-located and consumed by a single evaluator call
   * (equivalent to {@code PARTITION BY} on those expressions). The default,
   * {@link Distributions#unspecified()}, imposes no requirement -- the splitting of the input
   * relation is then governed solely by the call site, and is undefined if the call site does not
   * specify one either.
   *
   * @return the required distribution; never null
   */
  default Distribution requiredDistribution() {
    return Distributions.unspecified();
  }

  /**
   * Returns the ordering this function requires of the rows within each input partition.
   * <p>
   * Spark sorts the rows within each partition by this ordering before passing them to
   * {@link TableFunctionEvaluator#eval(java.util.Iterator)}. The default is no required ordering,
   * in which case the order of rows within a partition is undefined unless the call site specifies
   * an {@code ORDER BY} clause.
   *
   * @return the required ordering; an empty array means no requirement
   */
  default SortOrder[] requiredOrdering() {
    return new SortOrder[0];
  }

  /**
   * Returns a serializable factory that produces the per-partition {@link TableFunctionEvaluator}.
   * Spark ships the factory to executors and calls {@link TableFunctionEvaluatorFactory#create()}
   * once per task.
   */
  TableFunctionEvaluatorFactory evaluatorFactory();

  /**
   * Returns the subset of the TABLE argument's columns this function's evaluator consumes, in the
   * order they should be presented to it.
   * <p>
   * Each reference must name a single top-level column of the TABLE argument's schema. Spark
   * projects only the referenced columns (in the given order) into the input relation, so
   * {@link TableFunctionEvaluator#eval(java.util.Iterator)} receives one field per reference and
   * the connector can prune columns it does not read. The default is an empty array, meaning no
   * pruning: the evaluator receives every column of the TABLE argument, in schema order.
   *
   * @return the selected input columns; an empty array means all columns (no pruning)
   */
  default NamedReference[] selectedInputColumns() {
    return new NamedReference[0];
  }
}
