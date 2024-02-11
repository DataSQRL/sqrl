package com.datasqrl.actions;

import static com.datasqrl.actions.InferGraphqlSchema.inferGraphQLSchema;
import static com.datasqrl.engine.server.ServerPhysicalPlan.getModelFileName;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.packager.config.ScriptFiles;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.serializer.Deserializer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import lombok.SneakyThrows;

public class WriteDeploymentArtifacts {

  private final SqrlFramework framework;
  @Named("buildDir")
  private final Path buildDir;
  @Named("targetDir")
  private final Path targetDir;
  private final ResourceResolver resourceResolver;
  private final CompilerConfiguration compilerConfig;
  private final ScriptFiles scriptFiles;

  @Inject
  public WriteDeploymentArtifacts(
      SqrlFramework framework, SqrlConfig config,
      ResourceResolver resourceResolver,
      CompilerConfiguration compilerConfig,
      ScriptFiles scriptFiles,
      @Named("buildDir") Path buildDir,
      @Named("targetDir") Path targetDir) {
    this.framework = framework;
    this.resourceResolver = resourceResolver;
    this.compilerConfig = compilerConfig;
    this.scriptFiles = scriptFiles;
    this.buildDir = buildDir;
    this.targetDir = targetDir;
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
