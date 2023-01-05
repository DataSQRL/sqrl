/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.spi.JacksonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.nio.file.Path;
import java.util.ServiceLoader;

public class Deserializer {

  final ObjectMapper jsonMapper;
  final YAMLMapper yamlMapper;

  public Deserializer() {
    SimpleModule module = new SimpleModule();
    ServiceLoader<JacksonDeserializer> moduleLoader = ServiceLoader.load(JacksonDeserializer.class);
    for (JacksonDeserializer deserializer : moduleLoader) {
      module.addDeserializer(deserializer.getSuperType(), deserializer);
    }

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
