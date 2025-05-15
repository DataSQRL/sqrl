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
package com.datasqrl.planner.graphql;

import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APISource;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.google.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class GenerateCoords {

  private final ErrorCollector errorCollector;

  public void generateCoordsAndUpdateServerPlan(
      Optional<APISource> source, ServerPhysicalPlan serverPlan) {
    var graphqlModelGenerator =
        new GraphqlModelGenerator2(
            serverPlan.getFunctions(), serverPlan.getMutations(), errorCollector);
    graphqlModelGenerator.walkAPISource(source.get());
    var model =
        RootGraphqlModel.builder()
            .queries(graphqlModelGenerator.getQueryCoords())
            .mutations(graphqlModelGenerator.getMutations())
            .subscriptions(graphqlModelGenerator.getSubscriptions())
            .schema(StringSchema.builder().schema(source.get().getSchemaDefinition()).build())
            .build();
    serverPlan.setModel(model);
  }
}
