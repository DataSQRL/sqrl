package com.datasqrl.actions;

import static com.datasqrl.actions.InferGraphqlSchema.inferGraphQLSchema;
import static com.datasqrl.engine.server.ServerPhysicalPlan.getModelFileName;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.config.TargetPath;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.ScriptConfiguration;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.serializer.Deserializer;
import com.google.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor(onConstructor_=@Inject)
public class WriteDeploymentArtifacts {

  private final SqrlFramework framework;
  private final GraphqlSourceFactory graphqlSource;

  private final BuildPath buildDir;
  private final TargetPath targetDir;
  private final CompilerConfiguration compilerConfig;

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
    APISource apiSchema = graphqlSource.get()
        .orElseGet(() ->
            new APISourceImpl(Name.system("<schema>"),
                inferGraphQLSchema(framework.getSchema(),
                    compilerConfig.isAddArguments())));

    writeSchema(buildDir, apiSchema.getSchemaDefinition());
  }

  @SneakyThrows
  private void writeSchema(Path rootDir, String schema) {
    Path schemaFile = rootDir.resolve(ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(schemaFile);
    Files.writeString(schemaFile, schema, StandardOpenOption.CREATE);
  }
}
