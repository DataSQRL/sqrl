package com.datasqrl.actions;

import java.util.Optional;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineType;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.GraphqlModelGenerator;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.plan.queries.APISource;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class GraphqlPostplanHook {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final APIConnectorManager apiManager;

  public Optional<RootGraphqlModel> updatePlan(Optional<APISource> source, PhysicalPlan physicalPlan) {
    if (pipeline.getStageByType(EngineType.SERVER).isEmpty()) {
      return Optional.empty();
    }

    Optional<RootGraphqlModel> root;
    if (source.isPresent()) {
      var modelGen = new GraphqlModelGenerator(
          framework.getCatalogReader().nameMatcher(), framework.getSchema(),
          physicalPlan.getDatabaseQueries(), framework.getQueryPlanner(), apiManager, physicalPlan);
      modelGen.walk(source.get());
      var model = RootGraphqlModel.builder().queries(modelGen.getCoords())
          .mutations(modelGen.getMutations()).subscriptions(modelGen.getSubscriptions())
          .schema(StringSchema.builder().schema(source.get().getSchemaDefinition()).build())
          .build();
      root = Optional.of(model);
      //todo remove
      physicalPlan.getPlans(ServerPhysicalPlan.class).findFirst().get().setModel(model);
    } else {
      root = Optional.empty();
    }
    return root;
  }
}
