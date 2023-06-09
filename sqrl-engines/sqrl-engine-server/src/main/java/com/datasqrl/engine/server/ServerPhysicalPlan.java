package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import lombok.SneakyThrows;
import lombok.Value;

@Value
public class ServerPhysicalPlan implements EnginePhysicalPlan {

  RootGraphqlModel model;
  JdbcDataSystemConnector jdbc;

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer) throws IOException {
    serializer.writeJson(deployDir.resolve(getModelFileName(stageName)), model, true);
    serializer.writeJson(deployDir.resolve(getConfigFilename(stageName)), jdbc, true);
  }

  public static ServerPhysicalPlan readFrom(Path deployDir, String stageName, Deserializer serializer) {
    return new ServerPhysicalPlan(
        serializer.mapJsonFile(deployDir.resolve(getModelFileName(stageName)), RootGraphqlModel.class),
        serializer.mapJsonFile(deployDir.resolve(getConfigFilename(stageName)), JdbcDataSystemConnector.class));
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
