package com.datasqrl.actions;

import static com.datasqrl.engine.server.ServerPhysicalPlan.getModelFileName;

import com.datasqrl.config.BuildPath;
import com.datasqrl.config.TargetPath;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.queries.APISource;
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


  private final BuildPath buildDir;
  private final TargetPath targetDir;

  public void run(Optional<RootGraphqlModel> model, Optional<APISource> graphqlSource, PhysicalPlan physicalPlan) {
    writeDeployArtifacts(physicalPlan, targetDir);
    graphqlSource.ifPresent(this::writeGraphqlSchema);
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

  public void writeGraphqlSchema(APISource source) {
    writeSchema(buildDir, source.getSchemaDefinition());
  }

  @SneakyThrows
  private void writeSchema(Path rootDir, String schema) {
    Path schemaFile = rootDir.resolve(ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(schemaFile);
    Files.writeString(schemaFile, schema, StandardOpenOption.CREATE);
  }
}
