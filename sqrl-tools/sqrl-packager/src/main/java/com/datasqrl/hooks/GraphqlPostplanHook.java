package com.datasqrl.hooks;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.GraphqlModelGenerator;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.inject.AutoBind;
import com.datasqrl.injector.PostplanHook;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.plan.queries.APISource;
import com.google.inject.Inject;
import graphql.schema.idl.SchemaParser;
import java.util.Map;
import java.util.Optional;

@AutoBind(PostplanHook.class)
public class GraphqlPostplanHook implements PostplanHook {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final Map<String, Optional<String>> scriptFiles;
  private final ResourceResolver resourceResolver;
  private final APIConnectorManager apiManager;

  @Inject
  public GraphqlPostplanHook(ExecutionPipeline pipeline, SqrlFramework framework,
      SqrlConfig config, ResourceResolver resourceResolver,
      APIConnectorManager apiManager) {
    this.pipeline = pipeline;
    this.framework = framework;

    scriptFiles = ScriptConfiguration.getFiles(config);
    this.resourceResolver = resourceResolver;
    this.apiManager = apiManager;
  }

  @Override
  public void runHook(PhysicalPlan plan) {
    throw new RuntimeException("tbd");
    //todo: migrate graphql inference to simpler two-step process
  }

  public Optional<RootGraphqlModel> run(PhysicalPlan physicalPlan) {
    if (pipeline.getStage(Type.SERVER).isEmpty()) {
      return Optional.empty();
    }

    Name graphqlName = Name.system(
        scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY).orElse("<schema>").split("\\.")[0]);

    Optional<APISource> apiSource = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
        .map(file -> APISource.of(file, framework.getNameCanonicalizer(), resourceResolver));

    Optional<RootGraphqlModel> root;
    if (apiSource.isPresent()) {
      GraphqlModelGenerator modelGen = new GraphqlModelGenerator(
          framework.getCatalogReader().nameMatcher(),
          framework.getSchema(), (new SchemaParser()).parse(apiSource.get().getSchemaDefinition()),
          apiSource.get(),
          physicalPlan.getDatabaseQueries(), framework.getQueryPlanner(), apiManager);
      modelGen.walk();
      RootGraphqlModel model = RootGraphqlModel.builder()
          .coords(modelGen.getCoords())
          .mutations(modelGen.getMutations())
          .subscriptions(modelGen.getSubscriptions())
          .schema(StringSchema.builder().schema(apiSource.get().getSchemaDefinition()).build())
          .build();
      root = Optional.of(model);
      //todo remove
      physicalPlan.getPlans(ServerPhysicalPlan.class).findFirst().get()
          .setModel(model);
    } else {
      root = Optional.empty();
    }

    return root;
  }
}
