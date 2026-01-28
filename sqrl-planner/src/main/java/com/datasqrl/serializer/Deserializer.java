/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.json.jackson.VertxModule;
import java.io.IOException;
import java.nio.file.Path;
import lombok.Getter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

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
            .registerModule(new VertxModule())
            .registerModule(module)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .addMixIn(LogicalType.class, AlphabeticMixin.class)
            .addMixIn(RowType.RowField.class, AlphabeticMixin.class)
            .addMixIn(TypeSerializer.class, AlphabeticMixin.class);

    yamlMapper = new YAMLMapper();
    yamlMapper
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .registerModule(module)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public <T> T mapJsonFile(Path path, Class<T> clazz) {
    return mapFile(jsonMapper, path, clazz);
  }

  public <T> T mapYAML(String yamlContent, Class<T> clazz) {
    return map(yamlMapper, yamlContent, clazz);
  }

  public <T> T mapYAMLFile(Path path, Class<T> clazz) {
    return mapFile(yamlMapper, path, clazz);
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
    var writer = jsonMapper.writer();
    if (pretty) {
      writer = writer.withDefaultPrettyPrinter();
    }
    writer.writeValue(file.toFile(), object);
  }

  public <O> String toJson(O object) throws IOException {
    return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
  }

  public <O> String writeYML(O object) throws IOException {
    return yamlMapper.writeValueAsString(object);
  }

  public <O> void writeYML(Path file, O object) throws IOException {
    var writer = yamlMapper.writer().withDefaultPrettyPrinter();
    writer.writeValue(file.toFile(), object);
  }

  @JsonPropertyOrder(alphabetic = true)
  private interface AlphabeticMixin {}
}
