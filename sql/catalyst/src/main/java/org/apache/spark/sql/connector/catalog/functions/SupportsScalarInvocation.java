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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.ScanBuilder;

/**
 * A mixin for a {@link BoundTableFunction} that is invoked with scalar arguments only and produces
 * its relation as a source.
 * <p>
 * The function returns a {@link ScanBuilder}, mirroring
 * {@link org.apache.spark.sql.connector.catalog.SupportsRead#newScanBuilder}: this lets it
 * participate in column pruning and filter pushdown (by implementing
 * {@link org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns} etc. on the returned
 * builder) and get distributed execution for free -- exactly like reading a table. For a small or
 * metadata-only result, the builder's {@code build()} may return a
 * {@link org.apache.spark.sql.connector.read.LocalScan}.
 *
 * @since 4.3.0
 */
@Evolving
public interface SupportsScalarInvocation extends BoundTableFunction {

  /**
   * Returns a scan builder for this table-valued function given the scalar arguments.
   * <p>
   * Spark validates and rearranges the call-site arguments so that the order and data types of the
   * fields in {@code scalarArgs} match those reported by {@link #parameters()}. The {@code Scan}
   * produced by the returned builder must report a {@code readSchema()} consistent with
   * {@link #resultSchema()} (after any column pruning the builder performs).
   *
   * @param scalarArgs the scalar argument values
   * @return a scan builder producing the function's rows
   */
  ScanBuilder newScanBuilder(InternalRow scalarArgs);
}
