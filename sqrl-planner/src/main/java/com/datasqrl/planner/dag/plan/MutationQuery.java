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
package com.datasqrl.planner.dag.plan;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.graphql.server.ResolvedMetadata;
import com.datasqrl.planner.util.Documented;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Represents a CREATE TABLE statement without a connector that is managed by DataSQRL and exposed
 * as a mutation in GraphQL.
 */
@Value
@Builder
public class MutationQuery implements ExecutableQuery, Documented {

  /** The name of the mutation */
  Name name;

  /** Stage against which the mutation is executed */
  ExecutionStage stage;

  /** The topic that the mutation is written into */
  EngineCreateTable createTopic;

  /** The data type of the input data for the mutation */
  RelDataType inputDataType;

  /** The data type of the result data for the mutation */
  RelDataType outputDataType;

  /** The columns that are computed and not provided explicitly by the user */
  @Singular Map<String, ResolvedMetadata> computedColumns;

  /** A documentation string that describes the mutation */
  @Default Optional<String> documentation = Optional.empty();

  /** How records are inserted into the topic for this mutation */
  @Default MutationInsertType insertType = MutationInsertType.SINGLE;

  /** Whether this mutation should be exposed in the interface */
  boolean generateAccess;
}
