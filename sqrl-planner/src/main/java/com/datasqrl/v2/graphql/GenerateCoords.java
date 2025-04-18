package com.datasqrl.v2.graphql;

import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.plan.queries.APISource;
import com.google.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class GenerateCoords {

  private final ErrorCollector errorCollector;

  public void generateCoordsAndUpdateServerPlan(Optional<APISource> source, ServerPhysicalPlan serverPlan) {
    GraphqlModelGenerator2 graphqlModelGenerator =
        new GraphqlModelGenerator2(serverPlan.getFunctions(), serverPlan.getMutations(), errorCollector);
    graphqlModelGenerator.walkAPISource(source.get());
    RootGraphqlModel model =
        RootGraphqlModel.builder()
            .queries(graphqlModelGenerator.getQueryCoords())
            .mutations(graphqlModelGenerator.getMutations())
            .subscriptions(graphqlModelGenerator.getSubscriptions())
            .schema(StringSchema.builder().schema(source.get().getSchemaDefinition()).build())
            .build();
    serverPlan.setModel(model);
  }
}
