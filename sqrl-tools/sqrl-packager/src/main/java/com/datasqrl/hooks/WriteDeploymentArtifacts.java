package com.datasqrl.hooks;

import static com.datasqrl.engine.server.ServerPhysicalPlan.getModelFileName;
import static com.datasqrl.hooks.GraphqlInferencePostcompileHook.inferGraphQLSchema;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.injector.PostplanHook;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;

public class WriteDeploymentArtifacts implements PostplanHook {

  private final SqrlFramework framework;
  private final Map<String, Optional<String>> scriptFiles;
  private final Path buildDir;
  private final Path targetDir;
  private final ResourceResolver resourceResolver;
  private final CompilerConfiguration compilerConfig;

  @Inject
  public WriteDeploymentArtifacts(
      SqrlFramework framework, SqrlConfig config,
      ResourceResolver resourceResolver,
      CompilerConfiguration compilerConfig,
      @Named("buildDir") Path buildDir,
      @Named("targetDir") Path targetDir) {

    this.framework = framework;
    this.resourceResolver = resourceResolver;
    this.compilerConfig = compilerConfig;

    scriptFiles = ScriptConfiguration.getFiles(config);
    this.buildDir = buildDir;
    this.targetDir = targetDir;
    Preconditions.checkArgument(!scriptFiles.isEmpty());
  }

  @Override
  public void runHook(PhysicalPlan plan) {
    throw new RuntimeException("tbd");
  }

  public void run(Optional<RootGraphqlModel> model, PhysicalPlan physicalPlan) {
    writeDeployArtifacts(physicalPlan, targetDir);
    writeGraphqlSchema();
    model.ifPresent(this::writeModel);
  }

  @SneakyThrows
  private void writeModel(RootGraphqlModel model) {
    new Deserializer().writeJson(targetDir.resolve(getModelFileName("server")), model, true);
  }

  @SneakyThrows
  public void writeDeployArtifacts(PhysicalPlan plan, Path deployDir) {
    plan.writeTo(deployDir, new Deserializer());
  }

  public void writeGraphqlSchema() {


    Name graphqlName = Name.system(
        scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY).orElse("<schema>").split("\\.")[0]);

    Optional<APISource> apiSchemaOpt = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
        .map(file -> APISource.of(file, framework.getNameCanonicalizer(), resourceResolver));

    APISource apiSchema = apiSchemaOpt.orElseGet(() -> new APISource(graphqlName,
        inferGraphQLSchema(framework.getSchema(), compilerConfig.isAddArguments())));

    writeSchema(buildDir, apiSchema.getSchemaDefinition());
  }

  @SneakyThrows
  private void writeSchema(Path rootDir, String schema) {
    Path schemaFile = rootDir.resolve(ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(schemaFile);
    Files.writeString(schemaFile, schema, StandardOpenOption.CREATE);
  }
}
