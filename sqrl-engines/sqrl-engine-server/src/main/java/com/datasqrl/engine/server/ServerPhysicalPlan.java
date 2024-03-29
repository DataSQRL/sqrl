package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ServerPhysicalPlan implements EnginePhysicalPlan {

  RootGraphqlModel model;
  ServerConfig config;

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer) throws IOException {
    serializer.writeJson(deployDir.resolve(getModelFileName(stageName)), model, true);
    Path resolve = deployDir.resolve(getConfigFilename(stageName));
    String jsonContent = config.toJson().encodePrettily();
    Files.write(resolve, jsonContent.getBytes(StandardCharsets.UTF_8));
  }

  public static final String MODEL_FILENAME_SUFFIX = "-model.json";
  public static final String CONFIG_FILENAME_SUFFIX = "-config.json";

  public static String getModelFileName(String stageName) {
    return stageName + MODEL_FILENAME_SUFFIX;
  }

  public static String getConfigFilename(String stageName) {
    return stageName + CONFIG_FILENAME_SUFFIX;
  }

}
