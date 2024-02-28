package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.graphql.inference.GraphqlModelGenerator;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISourceImpl;
import com.google.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class GraphqlPostplanHook {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final ResourceResolver resourceResolver;
  private final APIConnectorManager apiManager;
  private final ScriptFiles scriptFiles;

  public Optional<RootGraphqlModel> run(Optional<APISource> source, PhysicalPlan physicalPlan) {
    if (pipeline.getStage(Type.SERVER).isEmpty()) {
      return Optional.empty();
    }

    Optional<RootGraphqlModel> root;
    if (source.isPresent()) {
      GraphqlModelGenerator modelGen = new GraphqlModelGenerator(
          framework.getCatalogReader().nameMatcher(), framework.getSchema(),
          physicalPlan.getDatabaseQueries(), framework.getQueryPlanner(), apiManager);
      modelGen.walk(source.get());
      RootGraphqlModel model = RootGraphqlModel.builder().coords(modelGen.getCoords())
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
