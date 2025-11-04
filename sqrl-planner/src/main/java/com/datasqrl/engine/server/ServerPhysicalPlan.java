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
package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.planner.dag.plan.MutationQuery;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ServerPhysicalPlan implements EnginePhysicalPlan {

  /** The endpoint functions for the server plan */
  @JsonIgnore final List<SqrlTableFunction> functions;

  /** The mutation endpoints */
  @JsonIgnore final List<MutationQuery> mutations;

  /** Additional server configuration */
  @JsonIgnore final List<DeploymentArtifact> deploymentArtifacts;

  /**
   * The generated API for the server. This gets generated after the planning and is added to the
   * plan later.
   */
  final Map<String, RootGraphqlModel> models = new LinkedHashMap<>();
}
