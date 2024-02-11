package com.datasqrl.hooks;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.inject.AutoBind;
import com.datasqrl.injector.PrecompileHook;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.plan.queries.APISource;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.Map;
import java.util.Optional;

@AutoBind(PrecompileHook.class)
public class AnalyzeGqlSchemaHook implements PrecompileHook {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final SqrlConfig config;
  private final ResourceResolver resourceResolver;
  private final APIConnectorManager apiManager;
  private final ModuleLoader moduleLoader;

  @Inject
  public AnalyzeGqlSchemaHook(ExecutionPipeline pipeline, SqrlFramework framework,
      SqrlConfig config, ResourceResolver resourceResolver,
      APIConnectorManager apiManager, ModuleLoader moduleLoader) {
    this.pipeline = pipeline;

    this.framework = framework;
    this.config = config;
    this.resourceResolver = resourceResolver;
    this.apiManager = apiManager;
    this.moduleLoader = moduleLoader;
  }

  @Override
  public void runHook() {
    if (pipeline.getStage(Type.SERVER).isEmpty()) {
      return;
    }

    Map<String, Optional<String>> scriptFiles = ScriptConfiguration.getFiles(config);
    Preconditions.checkArgument(!scriptFiles.isEmpty());
    Optional<String> graphqlSchema = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY);

    analyzeSchema(graphqlSchema);

    apiManager.updateModuleLoader(moduleLoader);
  }

  public void analyzeSchema(Optional<String> graphqlSchema) {
    GraphQLMutationExtraction preAnalysis = new GraphQLMutationExtraction(
        framework.getTypeFactory(), framework.getNameCanonicalizer());
    Optional<APISource> apiSchemaOpt = graphqlSchema
        .map(file -> APISource.of(file, framework.getNameCanonicalizer(), resourceResolver));
    apiSchemaOpt.ifPresent(api -> preAnalysis.analyze(api, apiManager));
  }
}
