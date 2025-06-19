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
package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ResourceFileUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/** Jackson-based implementation of {@link SqrlConfig} with JSON Schema validation and merging. */
public class SqrlConfig {
  public static final int CURRENT_VERSION = 1;
  public static final String VERSION_KEY = "version";

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .findAndRegisterModules()
          .setVisibility(
              com.fasterxml.jackson.annotation.PropertyAccessor.FIELD,
              com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY)
          .configure(
              com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
              false)
          .configure(
              com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

  private final ErrorCollector errors;
  private final ObjectNode root;
  private final String configFilename;
  private final String prefix;

  private SqrlConfig(ErrorCollector errors, ObjectNode root, String configFilename, String prefix) {
    this.errors = errors;
    this.root = root;
    this.configFilename = configFilename;
    this.prefix = prefix;
  }

  /** Create a new empty configuration with given version. */
  public static SqrlConfig create(ErrorCollector errors, int version) {
    ObjectNode base = MAPPER.createObjectNode();
    base.put(VERSION_KEY, version);
    return new SqrlConfig(errors, base, null, "");
  }

  /** Load configuration from a single JSON file. */
  public static SqrlConfig fromFiles(ErrorCollector errors, Path firstFile) {
    return getPackageConfig(errors, null, List.of(firstFile));
  }

  /** Load and merge package JSON files with schema validation. */
  public static PackageJson fromFilesPackageJson(ErrorCollector errors, List<Path> files) {
    return new PackageJsonImpl(getPackageConfig(errors, "/jsonSchema/packageSchema.json", files));
  }

  /** Load and merge publish package JSON files with schema validation. */
  public static PackageJson fromFilesPublishPackageJson(ErrorCollector errors, List<Path> files) {
    return new PackageJsonImpl(
        getPackageConfig(errors, "/jsonSchema/publishPackageSchema.json", files));
  }

  /** Validate a JSON file against the given schema resource. */
  public static boolean validateJsonFile(
      Path jsonFilePath, String schemaResourcePath, ErrorCollector errors) {
    if (schemaResourcePath == null) {
      return true;
    }
    ErrorCollector collector = errors.abortOnFatal(false);
    JsonNode json;
    try {
      json = MAPPER.readTree(jsonFilePath.toFile());
    } catch (IOException e) {
      collector.fatal("Could not read json file [%s]: %s", jsonFilePath, e);
      return false;
    }
    String schemaText = ResourceFileUtil.readResourceFileContents(schemaResourcePath);
    JsonNode schemaNode;
    try {
      schemaNode = MAPPER.readTree(schemaText);
    } catch (IOException e) {
      collector.fatal("Could not parse json schema file [%s]: %s", schemaResourcePath, e);
      return false;
    }
    JsonSchema schema = JsonSchemaFactory.getInstance(VersionFlag.V202012).getSchema(schemaNode);
    Set<ValidationMessage> messages =
        schema.validate(json, ctx -> ctx.getExecutionConfig().setFormatAssertionsEnabled(true));
    if (messages.isEmpty()) {
      return true;
    }
    messages.forEach(
        vm -> collector.fatal("%s at location [%s]", vm.getMessage(), vm.getInstanceLocation()));
    return false;
  }

  /** Load multiple JSON files and default-package.json, validate and merge them. */
  public static SqrlConfig getPackageConfig(
      ErrorCollector errors, String jsonSchemaResource, List<Path> files) {
    boolean valid = true;
    List<JsonNode> jsons = new ArrayList<>();
    for (int i = files.size() - 1; i >= 0; i--) {
      Path file = files.get(i);
      ErrorCollector local = errors.withConfig(file.toString());
      valid &= validateJsonFile(file, jsonSchemaResource, local);
      try {
        jsons.add(MAPPER.readTree(file.toFile()));
      } catch (IOException e) {
        throw local.exception("Could not parse JSON file [%s]: %s", file, e.toString());
      }
    }
    try {
      URL url = SqrlConfigCommons.class.getResource("/default-package.json");
      if (url != null) {
        jsons.add(MAPPER.readTree(url));
      }
    } catch (IOException e) {
      throw errors.exception("Error loading default configuration: %s", e.toString());
    }
    if (!valid) {
      throw errors.exception("Configuration file invalid: %s", files);
    }
    ObjectNode merged = MAPPER.createObjectNode();
    jsons.forEach(node -> merge(merged, node));
    String configName = files.isEmpty() ? "default-package.json" : files.get(0).toString();
    return new SqrlConfig(errors.withConfig(configName), merged, configName, "");
  }

  /** Load configuration from a URL. */
  public static SqrlConfig fromURL(ErrorCollector errors, URL url) {
    JsonNode node;
    try {
      node = MAPPER.readTree(url);
    } catch (IOException e) {
      throw errors
          .withConfig(url.toString())
          .exception("Could not read JSON from URL [%s]: %s", url, e.toString());
    }
    ObjectNode merged = MAPPER.createObjectNode();
    merge(merged, node);
    return new SqrlConfig(errors.withConfig(url.toString()), merged, url.toString(), "");
  }

  /** Load configuration from a raw JSON string. */
  public static SqrlConfig fromString(ErrorCollector errors, String string) {
    JsonNode node;
    try {
      node = MAPPER.readTree(string);
    } catch (IOException e) {
      throw errors.withConfig("local").exception("Could not parse JSON string: %s", e.toString());
    }
    ObjectNode merged = MAPPER.createObjectNode();
    merge(merged, node);
    return new SqrlConfig(errors.withConfig("local"), merged, "local", "");
  }

  private static void merge(ObjectNode target, JsonNode update) {
    update
        .fields()
        .forEachRemaining(
            entry -> {
              JsonNode existing = target.get(entry.getKey());
              if (existing instanceof ObjectNode && entry.getValue().isObject()) {
                merge((ObjectNode) existing, entry.getValue());
              } else {
                target.set(entry.getKey(), entry.getValue());
              }
            });
  }

  private JsonNode node() {
    if (prefix.isEmpty()) {
      return root;
    }
    JsonNode n = root;
    for (String seg : prefix.split("\\.")) {
      if (n.has(seg)) {
        n = n.get(seg);
      } else {
        return null;
      }
    }
    return n;
  }

  private String getFullKey(String key) {
    return prefix.isEmpty() ? key : prefix + "." + key;
  }

  public int getVersion() {
    errors.checkFatal(containsKey(VERSION_KEY), "Configuration file does not have a `version`.");
    int version = asInt(VERSION_KEY).get();
    errors.checkFatal(version > 0, "Invalid version: %s", version);
    return version;
  }

  public SqrlConfig getSubConfig(String name) {
    return new SqrlConfig(errors, root, configFilename, getFullKey(name));
  }

  public boolean hasSubConfig(String name) {
    JsonNode n = node();
    return n != null && n.has(name) && n.get(name).isObject();
  }

  public void validateSubConfig(String name) {
    errors.checkFatal(
        hasSubConfig(name), "Missing sub-configuration under key: %s", getFullKey(name));
  }

  public Iterable<String> getKeys() {
    JsonNode n = node();
    if (n == null || !n.isObject()) {
      return Collections.emptyList();
    }
    List<String> keys = new ArrayList<>();
    n.fieldNames().forEachRemaining(keys::add);
    return keys;
  }

  public boolean containsKey(String key) {
    JsonNode n = node();
    return n != null && n.has(key) && !n.get(key).isContainerNode();
  }

  public <T> Value<T> as(String key, Class<T> clazz) {
    String fullKey = getFullKey(key);
    JsonNode n = node();
    T value = null;
    if (n != null && n.has(key)) {
      try {
        value = MAPPER.treeToValue(n.get(key), clazz);
      } catch (Exception e) {
        throw errors.exception("Could not parse key [%s] as %s: %s", fullKey, clazz, e.toString());
      }
    }
    return new ValueImpl<>(fullKey, errors, value);
  }

  public <T> Value<T> allAs(Class<T> clazz) {
    JsonNode n = node();
    errors.checkFatal(
        n != null && n.isObject(),
        "Cannot map configuration onto a non-object: %s",
        clazz.getName());
    T value;
    try {
      value = MAPPER.convertValue(n, clazz);
    } catch (IllegalArgumentException e) {
      throw errors.exception(
          "Could not map configuration values on object of clazz [%s]: %s",
          clazz.getName(), e.toString());
    }
    return new ValueImpl<>(prefix, errors, value);
  }

  public <T> Value<List<T>> asList(String key, Class<T> clazz) {
    JsonNode n = node();
    List<T> list = List.of();
    if (n != null && n.has(key) && n.get(key).isArray()) {
      list = new ArrayList<>();
      for (JsonNode element : n.get(key)) {
        list.add(MAPPER.convertValue(element, clazz));
      }
    }
    return new ValueImpl<>(getFullKey(key), errors, list);
  }

  public <T> Value<Map<String, T>> asMap(String key, Class<T> clazz) {
    JsonNode n = node();
    Map<String, T> map = new LinkedHashMap<>();
    if (n != null && n.has(key) && n.get(key).isObject()) {
      n.get(key)
          .fields()
          .forEachRemaining(e -> map.put(e.getKey(), MAPPER.convertValue(e.getValue(), clazz)));
    }
    return new ValueImpl<>(getFullKey(key), errors, map);
  }

  public ErrorCollector getErrorCollector() {
    return errors;
  }

  public SqrlConfig setProperty(String key, Object value) {
    String[] parts = getFullKey(key).split("\\.");
    ObjectNode curr = root;
    for (int i = 0; i < parts.length - 1; i++) {
      JsonNode child = curr.get(parts[i]);
      if (!(child instanceof ObjectNode)) {
        ObjectNode obj = MAPPER.createObjectNode();
        curr.set(parts[i], obj);
        curr = obj;
      } else {
        curr = (ObjectNode) child;
      }
    }
    curr.set(parts[parts.length - 1], MAPPER.valueToTree(value));
    return this;
  }

  public void setProperties(Object value) {
    JsonNode tree = MAPPER.valueToTree(value);
    errors.checkFatal(
        tree.isObject(),
        "Cannot set multiple properties from non-object: %s",
        value.getClass().getName());
    ObjectNode curr = (ObjectNode) node();
    tree.fields().forEachRemaining(e -> curr.set(e.getKey(), e.getValue()));
  }

  public void copy(SqrlConfig from) {
    errors.checkFatal(from instanceof SqrlConfig, "Cannot copy config from other impl");
    SqrlConfig other = (SqrlConfig) from;
    com.fasterxml.jackson.databind.JsonNode sub = other.node();
    if (sub instanceof com.fasterxml.jackson.databind.node.ObjectNode) {
      sub.fields().forEachRemaining(e -> setProperty(e.getKey(), e.getValue()));
    }
  }

  public String toString() {
    return "SqrlConfig{" + "configFilename='" + configFilename + '\'' + '}';
  }

  public void toFile(Path file) {
    toFile(file, true);
  }

  public void toFile(Path file, boolean pretty) {
    ObjectNode toWrite;
    if (prefix.isEmpty()) {
      toWrite = root;
    } else {
      JsonNode n = node();
      errors.checkFatal(n != null && n.isObject(), "Cannot write non-object subConfig to file");
      toWrite = (ObjectNode) n;
    }
    try {
      String text =
          pretty
              ? MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toWrite)
              : MAPPER.writeValueAsString(toWrite);
      Files.writeString(file, text, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw errors
          .withConfig(file)
          .exception("Could not write configuration to file: %s", e.toString());
    }
  }

  public Map<String, Object> toMap() {
    return MAPPER.convertValue(node() == null ? MAPPER.createObjectNode() : node(), Map.class);
  }

  public Map<String, String> toStringMap() {
    Map<String, Object> map = toMap();
    LinkedHashMap<String, String> out = new LinkedHashMap<>();
    map.forEach((k, v) -> out.put(k, String.valueOf(v)));
    return out;
  }

  public SerializedSqrlConfig serialize() {
    Map<String, Object> map = new LinkedHashMap<>();
    node().fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue()));
    return new Serialized(configFilename, map, prefix);
  }

  /** Implementation of Value<T> backed by a concrete value. */
  private static class ValueImpl<T> implements Value<T> {
    private final String fullKey;
    private final ErrorCollector errors;
    private final T property;
    private T defaultValue;
    private final Map<Predicate<T>, String> validators = new LinkedHashMap<>();

    ValueImpl(String fullKey, ErrorCollector errors, T property) {
      this.fullKey = fullKey;
      this.errors = errors;
      this.property = property;
    }

    public T get() {
      boolean has = property != null || defaultValue != null;
      errors.checkFatal(has, "Could not find key [%s] in configuration", fullKey);
      T value = property != null ? property : defaultValue;
      for (Map.Entry<Predicate<T>, String> e : validators.entrySet()) {
        errors.checkFatal(
            e.getKey().test(value),
            "Value [%s] for key [%s] is not valid. %s",
            value,
            fullKey,
            e.getValue());
      }
      return value;
    }

    public Value<T> withDefault(T defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Value<T> validate(Predicate<T> validator, String msg) {
      validators.put(validator, msg);
      return this;
    }

    public Value<T> map(Function<T, T> mapFunction) {
      T mapped = property != null ? mapFunction.apply(property) : null;
      return new ValueImpl<>(fullKey, errors, mapped).withDefault(defaultValue);
    }
  }

  @AllArgsConstructor
  @Getter
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  public static class Serialized implements SerializedSqrlConfig {

    private String configFilename;
    private Map<String, Object> configs;
    private String prefix;

    public SqrlConfig deserialize(@NonNull ErrorCollector errors) {
      ObjectNode rootNode = MAPPER.createObjectNode();
      configs.forEach((k, v) -> rootNode.set(k, MAPPER.valueToTree(v)));
      ErrorCollector configErrors = errors;
      if (configFilename != null && !configFilename.isBlank()) {
        configErrors = configErrors.withConfig(configFilename);
      }
      return new SqrlConfig(configErrors, rootNode, configFilename, prefix);
    }
  }

  public boolean hasKey(String key) {
    return containsKey(key);
  }

  public Value<String> asString(String key) {
    return as(key, String.class).map(String::trim);
  }

  public Value<Long> asLong(String key) {
    return as(key, Long.class);
  }

  public Value<Integer> asInt(String key) {
    return as(key, Integer.class);
  }

  public Value<Boolean> asBool(String key) {
    return as(key, Boolean.class);
  }

  interface Value<T> {

    T get();

    default Optional<T> getOptional() {
      var value = this.withDefault(null).get();
      if (value == null) {
        return Optional.empty();
      } else {
        return Optional.of(value);
      }
    }

    Value<T> withDefault(T defaultValue);

    Value<T> validate(Predicate<T> validator, String msg);

    Value<T> map(Function<T, T> mapFunction);
  }

  static <T extends Enum<T>> T getEnum(
      Value<String> value, Class<T> clazz, Optional<T> defaultValue) {
    if (defaultValue.isPresent()) {
      value = value.withDefault(defaultValue.get().name());
    }
    return Enum.valueOf(
        clazz,
        value
            .map(String::toLowerCase)
            .validate(
                v -> isEnumValue(v, clazz), "Use one of: %s".formatted(clazz.getEnumConstants()))
            .get());
  }

  public static <T extends Enum<T>> boolean isEnumValue(String value, Class<T> clazz) {
    for (T e : clazz.getEnumConstants()) {
      if (e.name().equals(value)) {
        return true;
      }
    }
    return false;
  }

  public static SqrlConfig createCurrentVersion() {
    return createCurrentVersion(ErrorCollector.root());
  }

  public static SqrlConfig createCurrentVersion(ErrorCollector errors) {
    return create(errors, CURRENT_VERSION);
  }

  public static SqrlConfig create(SqrlConfig other) {
    return create(other.getErrorCollector(), other.getVersion());
  }
}
