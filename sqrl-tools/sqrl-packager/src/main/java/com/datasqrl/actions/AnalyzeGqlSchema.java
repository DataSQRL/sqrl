package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.plan.queries.APISource;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class AnalyzeGqlSchema {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final SqrlConfig config;
  private final ResourceResolver resourceResolver;
  private final APIConnectorManager apiManager;
  private final ModuleLoader moduleLoader;

  public void run() {
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
