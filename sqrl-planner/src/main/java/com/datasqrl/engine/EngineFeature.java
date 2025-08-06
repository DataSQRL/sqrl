/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine;

import java.util.EnumSet;

public enum EngineFeature {

  // Supports mutations
  MUTATIONS,
  // Engine supports denormalized (or nested) data
  DENORMALIZE,
  // Engine supports temporal joins of any sort
  TEMPORAL_JOIN,
  // Engine supports temporal joins against arbitrary state tables (not just deduplicated streams)
  TEMPORAL_JOIN_ON_STATE,
  // Engine supports converting state tables into change streams
  TO_STREAM,
  // Engine supports stream window aggregation
  STREAM_WINDOW_AGGREGATION,
  // Engine supports queries and filters based on query time
  NOW,
  // Engine supports global sort
  GLOBAL_SORT,
  // Engine supports window functions with multiple ranks
  MULTI_RANK,
  // Engine supports the extended SQRL function catalog
  EXTENDED_FUNCTIONS,
  // Engine supports user defined functions
  CUSTOM_FUNCTIONS,
  // Engine can inline table functions
  TABLE_FUNCTION_SCAN,
  // Engine can export data to TableSink
  EXPORT,
  // Writing/upserting data into engine by primary key will deduplicate data
  MATERIALIZE_ON_KEY,
  // Engine supports relations (i.e. no primary key)
  RELATIONS,
  // Engine supports partitioning of data
  PARTITIONING,
  // Engine supports partitioned writing of data
  PARTITIONED_WRITES,
  // Engine requires that the primary key is not null
  REQUIRES_NOT_NULL_PRIMARY_KEY,
  // Datastore engine support access to data without partition key
  ACCESS_WITHOUT_PARTITION;

  public static EnumSet<EngineFeature> STANDARD_STREAM =
      EnumSet.of(
          DENORMALIZE,
          TEMPORAL_JOIN,
          TO_STREAM,
          STREAM_WINDOW_AGGREGATION,
          EXTENDED_FUNCTIONS,
          CUSTOM_FUNCTIONS,
          EXPORT);

  public static EnumSet<EngineFeature> STANDARD_DATABASE =
      EnumSet.of(
          NOW,
          GLOBAL_SORT,
          MATERIALIZE_ON_KEY,
          MULTI_RANK,
          TABLE_FUNCTION_SCAN,
          RELATIONS,
          DENORMALIZE,
          PARTITIONING,
          REQUIRES_NOT_NULL_PRIMARY_KEY);

  public static EnumSet<EngineFeature> STANDARD_TABLE_FORMAT =
      EnumSet.of(
          MATERIALIZE_ON_KEY,
          DENORMALIZE,
          PARTITIONING,
          PARTITIONED_WRITES,
          ACCESS_WITHOUT_PARTITION);

  public static EnumSet<EngineFeature> STANDARD_QUERY =
      EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK, TABLE_FUNCTION_SCAN, RELATIONS);

  public static EnumSet<EngineFeature> NO_CAPABILITIES = EnumSet.noneOf(EngineFeature.class);
}
