package com.datasqrl.loaders;

import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.nio.file.Path;

public class Deserializer {

  final ObjectMapper jsonMapper;
  final YAMLMapper yamlMapper;

  public Deserializer() {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(DataSystemConnectorConfig.class,
        new DataSystemConnectorConfig.Deserializer());
    module.addDeserializer(DataSystemDiscoveryConfig.class,
        new DataSystemDiscoveryConfig.Deserializer());
    jsonMapper = new ObjectMapper().registerModule(module);
    yamlMapper = new YAMLMapper();
  }

  public <T> T mapJsonFile(Path path, Class<T> clazz) {
    return mapFile(jsonMapper, path, clazz);
  }

  public <T> T mapYAMLFile(Path path, Class<T> clazz) {
    return mapFile(yamlMapper, path, clazz);
  }

  public static <T> T mapFile(ObjectMapper mapper, Path path, Class<T> clazz) {
    try {
      return mapper.readValue(path.toFile(), clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
