package com.datasqrl.graphql.config;

import com.datasqrl.util.JsonMergeUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.VertxModule;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ServerConfigUtil {

  @SuppressWarnings("unchecked")
  @SneakyThrows
  public static ServerConfig mergeConfigs(
      ObjectMapper objectMapper, ServerConfig serverConfig, Map<String, Object> configOverrides) {
    objectMapper = objectMapper.copy().registerModule(new VertxModule());
    var config = ((ObjectNode) objectMapper.valueToTree(serverConfig)).deepCopy();
    JsonMergeUtils.merge(config, objectMapper.valueToTree(configOverrides));
    var json = objectMapper.treeToValue(config, Map.class);
    return new ServerConfig(new JsonObject(json));
  }
}
