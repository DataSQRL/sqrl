/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.serializer;

import com.datasqrl.module.resolver.ResourceResolver;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import lombok.Getter;

@Getter
public class Deserializer {
  public static Deserializer INSTANCE = new Deserializer();

  final ObjectMapper jsonMapper;
  final YAMLMapper yamlMapper;

  protected Deserializer() {
    SimpleModule module = new SqrlSerializerModule();
    jsonMapper =
        new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .registerModule(module)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    yamlMapper = new YAMLMapper();
    yamlMapper
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .registerModule(module)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public <T> T mapJsonFile(URI path, Class<T> clazz) {
    return mapFile(jsonMapper, path, clazz);
  }

  public <T> T mapJsonFile(Path path, Class<T> clazz) {
    return mapFile(jsonMapper, path, clazz);
  }

  public <T> T mapYAMLFile(URI uri, Class<T> clazz) {
    return mapFile(yamlMapper, uri, clazz);
  }

  public <T> T mapYAML(String yamlContent, Class<T> clazz) {
    return map(yamlMapper, yamlContent, clazz);
  }

  public <T> T mapYAMLFile(Path path, Class<T> clazz) {
    return mapFile(yamlMapper, path, clazz);
  }

  public static <T> T mapFile(ObjectMapper mapper, URI uri, Class<T> clazz) {
    try {
      return mapper.readValue(ResourceResolver.toURL(uri), clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T map(ObjectMapper mapper, String content, Class<T> clazz) {
    try {
      return mapper.readValue(content, clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T mapFile(ObjectMapper mapper, Path path, Class<T> clazz) {
    try {
      return mapper.readValue(path.toFile(), clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <O> void writeJson(Path file, O object) throws IOException {
    writeJson(file, object, false);
  }

  public <O> void writeJson(Path file, O object, boolean pretty) throws IOException {
    ObjectWriter writer = jsonMapper.writer();
    if (pretty) writer = writer.withDefaultPrettyPrinter();
    writer.writeValue(file.toFile(), object);
  }

  public <O> String toJson(O object) throws IOException {
    return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
  }

  public <O> String writeYML(O object) throws IOException {
    return yamlMapper.writeValueAsString(object);
  }

  public <O> void writeYML(Path file, O object) throws IOException {
    ObjectWriter writer = yamlMapper.writer().withDefaultPrettyPrinter();
    writer.writeValue(file.toFile(), object);
  }
}
