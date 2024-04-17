package com.datasqrl.engine.server;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.SqrlObjectMapper;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

@AllArgsConstructor
@Getter
public class ServerPhysicalPlan implements EnginePhysicalPlan {

  @Setter
  RootGraphqlModel model;
  ServerConfig config;

  @SneakyThrows
  public String getModelJson() {
    return SqrlObjectMapper.mapper.writeValueAsString(model);
  }
}
