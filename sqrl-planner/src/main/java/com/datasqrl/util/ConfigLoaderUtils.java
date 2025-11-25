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
package com.datasqrl.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.engine.log.kafka.KafkaPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.error.ResourceFileUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.dialect.Dialects;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.types.Either;

/** Utility class to load different kind of configurations during CLI process execution. */
@Slf4j
public final class ConfigLoaderUtils {

  public static final String PACKAGE_SCHEMA_PATH = "/jsonSchema/packageSchema.json";

  public static final ObjectMapper MAPPER =
      new ObjectMapper()
          .setVisibility(
              com.fasterxml.jackson.annotation.PropertyAccessor.FIELD,
              com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY)
          .configure(
              com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
              false)
          .configure(
              com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

  private static final List<String> DEFAULTS = List.of("/default-package.json");
  private static final List<String> RUN_DEFAULTS =
      Stream.concat(DEFAULTS.stream(), Stream.of("/default-run-package.json")).toList();

  /**
   * Loads a resolved SQRL {@code package.json} from an already compiled project.
   *
   * @param buildDir build directory of a compiled SQRL project
   * @return the loaded config as a {@link PackageJson}
   */
  public static PackageJson loadResolvedConfig(ErrorCollector errors, Path buildDir) {
    checkArgument(
        Files.isDirectory(buildDir),
        String.format(
            "Failed to load %s, dir '%s' does not exist", SqrlConstants.PACKAGE_JSON, buildDir));

    return loadResolvedConfigFromFile(errors, buildDir.resolve(SqrlConstants.PACKAGE_JSON));
  }

  /**
   * Loads SQRL config from a resolved JSON file.
   *
   * @param errors collector to handle errors
   * @param packageJson resolved JSON file to load
   * @return the loaded config as a {@link PackageJson}
   */
  public static PackageJson loadResolvedConfigFromFile(ErrorCollector errors, Path packageJson) {
    return loadResolvedConfigFromFile(errors, packageJson, PACKAGE_SCHEMA_PATH);
  }

  static PackageJson loadResolvedConfigFromFile(
      ErrorCollector errors, Path packageJson, String packageSchemaPath) {
    checkArgument(
        Files.isRegularFile(packageJson),
        String.format("Failed to load %s, it is not a regular file", packageJson));

    var objectNode = convertFileToObjectNode(errors, Either.Left(packageJson));

    return SqrlConfig.loadResolvedConfig(errors, objectNode, packageSchemaPath);
  }

  /**
   * Loads the default SQRL config.
   *
   * @param errors collector to handle errors
   * @return the loaded config as a {@link PackageJson}
   */
  public static PackageJson loadDefaultConfig(ErrorCollector errors) {
    return loadUnresolvedConfig(errors, List.of());
  }

  /**
   * Loads the default SQRL config, and overwrites that with the content of any given JSON file. Any
   * upcoming file will overwrite previous values for any key that were already present.
   *
   * @param errors collector to handle errors
   * @param files additional JSON file(s)
   * @return the loaded and merged config as a {@link PackageJson}
   */
  public static PackageJson loadUnresolvedConfig(ErrorCollector errors, List<Path> files) {
    return loadUnresolvedConfig(errors, files, DEFAULTS);
  }

  /**
   * Loads the default SQRL run config, and overwrites that with the content of any given JSON file.
   * Any upcoming file will overwrite previous values for any key that were already present.
   *
   * @param errors collector to handle errors
   * @param files additional JSON file(s)
   * @return the loaded and merged config as a {@link PackageJson}
   */
  public static PackageJson loadUnresolvedRunConfig(ErrorCollector errors, List<Path> files) {
    return loadUnresolvedConfig(errors, files, RUN_DEFAULTS);
  }

  /**
   * Validate a JSON file against the given schema resource. If not schema is given, then no
   * validation happens.
   *
   * @param errors collector to handle errors
   * @param json JSON to validate
   * @param jsonSchemaResource schema to match
   * @return {@code true} if the given JSON matches the schema or there were no given schema, {@code
   *     false} otherwise
   */
  public static boolean isValidJson(
      ErrorCollector errors, JsonNode json, @Nullable String jsonSchemaResource) {
    if (jsonSchemaResource == null) {
      return true;
    }

    var collector = errors.abortOnFatal(false);
    var schemaText = ResourceFileUtil.readResourceFileContents(jsonSchemaResource);
    JsonNode schemaNode;
    try {
      schemaNode = MAPPER.readTree(schemaText);
    } catch (IOException e) {
      collector.fatal("Could not parse json schema file [%s]: %s", jsonSchemaResource, e);
      return false;
    }

    var schemaRegistryConf = SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build();
    var schema =
        SchemaRegistry.withDefaultDialect(
                Dialects.getDraft202012(),
                builder -> builder.schemaRegistryConfig(schemaRegistryConf))
            .getSchema(schemaNode);

    var messages = schema.validate(json);
    if (messages.isEmpty()) {
      return true;
    }

    messages.forEach(
        vm -> collector.fatal("%s at location [%s]", vm.getMessage(), vm.getInstanceLocation()));

    return false;
  }

  /**
   * Loads Flink configuration from a {@code flink-config.yaml} file that is assumed to exist inside
   * the given plan directory.
   *
   * @param planDir plan directory that contains tha YAML
   * @return Flink {@link Configuration} with loaded config
   * @throws IOException if any internal file operation fails
   */
  public static Configuration loadFlinkConfig(Path planDir) throws IOException {
    validatePlanDir(planDir);

    var confFile = planDir.resolve("flink-config.yaml");
    checkArgument(
        confFile.toFile().exists() && confFile.toFile().isFile(),
        "Failed to load Flink config, 'flink-config.yaml' does not found.");

    var tempDir = Files.createTempDirectory("flink-conf");
    try {
      var targetFile = tempDir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME);
      Files.copy(confFile, targetFile);

      return GlobalConfiguration.loadConfiguration(tempDir.toString());
    } finally {
      FileUtils.forceDelete(tempDir.toFile());
    }
  }

  /**
   * Loads the Kafka physical plan from the plan directory by parsing the {@code kafka.json}
   * configuration file. This method constructs a complete {@link KafkaPhysicalPlan} containing both
   * regular topics and test runner topics.
   *
   * @param planDir the plan directory containing the {@code kafka.json} file
   * @return an {@link Optional} containing a {@link KafkaPhysicalPlan} with topics and test runner
   *     topics, or empty if no {@code kafka.json} file exists
   * @throws IllegalArgumentException if the plan directory is null or does not exist
   * @throws IllegalStateException if the {@code kafka.json} file exists but cannot be parsed
   */
  public static Optional<KafkaPhysicalPlan> loadKafkaPhysicalPlan(Path planDir) {
    validatePlanDir(planDir);

    var kafkaFile = planDir.resolve("kafka.json").toFile();
    if (kafkaFile.exists()) {

      try {
        var kafkaPlan = MAPPER.readValue(kafkaFile, KafkaPhysicalPlan.class);
        return Optional.of(kafkaPlan);

      } catch (Exception ex) {
        throw new IllegalStateException(String.format("Failed to load '%s'", kafkaFile), ex);
      }
    }

    return Optional.empty();
  }

  /**
   * Loads PostgreSQL physical plan from the plan directory by parsing the {@code postgres.json}
   * configuration file. This method constructs a complete {@link JdbcPhysicalPlan} containing
   * database statements for schema creation, views, indexes, and other database artifacts.
   *
   * @param planDir the plan directory containing the {@code postgres.json} file
   * @return an {@link Optional} containing a {@link JdbcPhysicalPlan} with database statements, or
   *     empty if no {@code postgres.json} file exists
   * @throws IllegalArgumentException if the plan directory is null or does not exist
   * @throws IllegalStateException if the {@code postgres.json} file exists but cannot be parsed
   */
  public static Optional<JdbcPhysicalPlan> loadPostgresPhysicalPlan(Path planDir) {
    validatePlanDir(planDir);

    var postgresFile = planDir.resolve("postgres.json").toFile();
    if (!postgresFile.exists()) {
      return Optional.empty();
    }

    try {
      var jdbcPlan = MAPPER.readValue(postgresFile, JdbcPhysicalPlan.class);
      return Optional.of(jdbcPlan);

    } catch (Exception ex) {
      throw new IllegalStateException(String.format("Failed to load '%s'", postgresFile), ex);
    }
  }

  @VisibleForTesting
  static PackageJson loadUnresolvedConfig(
      ErrorCollector errors, List<Path> files, List<String> defaults) {
    var valid = true;
    var jsons = new ArrayList<ObjectNode>();

    // Convert, validate, and add defaults
    for (String defaultPath : defaults) {
      var url = ConfigLoaderUtils.class.getResource(defaultPath);
      if (url == null) {
        throw errors.withConfig(defaultPath).exception("Default configuration not found");
      }
      var defaultJson = convertFileToObjectNode(errors, Either.Right(url));
      valid &= isValidJson(errors, defaultJson, PACKAGE_SCHEMA_PATH);
      jsons.add(defaultJson);
    }

    // Convert, validate, and add files
    for (Path file : files) {
      var json = convertFileToObjectNode(errors, Either.Left(file));
      valid &= isValidJson(errors, json, PACKAGE_SCHEMA_PATH);
      jsons.add(json);
    }

    if (!valid) {
      throw errors.exception(
          errors
              .getErrors()
              .combineMessages(
                  ErrorMessage.Severity.FATAL, "Failed to load package configuration:\n\n", "\n"));
    }

    // Merge all collected JSON into one object
    var merged = MAPPER.createObjectNode();
    jsons.forEach(node -> JsonMergeUtils.merge(merged, node));

    return SqrlConfig.loadResolvedConfig(errors, merged);
  }

  private static void validatePlanDir(Path planDir) {
    checkArgument(
        Files.isDirectory(planDir), "Failed to load Flink config, plan dir does not exist.");
  }

  private static ObjectNode convertFileToObjectNode(ErrorCollector errors, Either<Path, URL> file) {
    var local = errors.withConfig(file.isLeft() ? file.left().toString() : file.right().toString());
    try {
      var jsonNode =
          file.isLeft() ? MAPPER.readTree(file.left().toFile()) : MAPPER.readTree(file.right());
      return (ObjectNode) jsonNode;

    } catch (IOException e) {
      throw local.exception("Could not parse JSON file [%s]: %s", file, e.toString());
    }
  }
}
