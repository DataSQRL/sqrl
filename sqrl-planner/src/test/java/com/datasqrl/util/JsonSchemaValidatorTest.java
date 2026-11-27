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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.planner.contract.odcs.OdcsConstants;
import com.datasqrl.util.JsonSchemaValidator.Violation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.keyword.UnknownKeywordFactory;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.SneakyThrows;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

class JsonSchemaValidatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * A schema that, like the ODCS one, keeps a definition in a group whose name is not a keyword of
   * JSON Schema
   */
  private static final String GROUPED_DEFINITIONS_SCHEMA = "/jsonSchema/definitions-in-groups.json";

  @Test
  @SneakyThrows
  void givenValidPackageJson_whenValidate_thenNoViolations() {
    var json = json("{ \"version\": \"1\", \"script\": { \"main\": \"main.sqrl\" } }");

    assertThat(JsonSchemaValidator.validate(json, ConfigLoaderUtils.PACKAGE_SCHEMA_PATH)).isEmpty();
  }

  @Test
  @SneakyThrows
  void givenInvalidPackageJson_whenValidate_thenReportsViolationWithPointer() {
    var json = json("{ \"version\": \"1\", \"script\": { \"main\": 5 } }");

    assertThat(JsonSchemaValidator.validate(json, ConfigLoaderUtils.PACKAGE_SCHEMA_PATH))
        .extracting(Violation::instancePointer)
        .containsExactly("/script/main");
  }

  @Test
  @SneakyThrows
  void givenMinimalContract_whenValidate_thenNoViolations() {
    var json =
        json(
            """
            {
              "apiVersion": "v3.2.0",
              "kind": "DataContract",
              "id": "customers",
              "version": "1.0.0",
              "schema": [
                { "name": "Customers", "properties": [ { "name": "id", "logicalType": "integer" } ] }
              ]
            }
            """);

    assertThat(JsonSchemaValidator.validate(json, OdcsConstants.JSON_SCHEMA_RESOURCE)).isEmpty();
  }

  @Test
  @SneakyThrows
  void givenContractWithWrongValues_whenValidate_thenReportsEachWithItsPointer() {
    var json =
        json(
            """
            {
              "apiVersion": "v3.2.0",
              "kind": "DataContract",
              "id": "customers",
              "version": "1.0.0",
              "schema": [
                {
                  "name": "Customers",
                  "properties": [
                    { "name": "id", "logicalType": "int" },
                    { "name": "email", "logicalType": "string", "required": "yes" }
                  ]
                }
              ]
            }
            """);

    assertThat(JsonSchemaValidator.validate(json, OdcsConstants.JSON_SCHEMA_RESOURCE))
        .extracting(Violation::instancePointer)
        .contains("/schema/0/properties/0/logicalType", "/schema/0/properties/1/required");
  }

  @Test
  @SneakyThrows
  void givenContractWithUnknownKey_whenValidate_thenEnforcesUnevaluatedProperties() {
    var json =
        json(
            """
            {
              "apiVersion": "v3.2.0",
              "kind": "DataContract",
              "id": "customers",
              "version": "1.0.0",
              "schema": [ { "name": "Customers", "columns": [] } ]
            }
            """);

    assertThat(JsonSchemaValidator.validate(json, OdcsConstants.JSON_SCHEMA_RESOURCE))
        .anySatisfy(
            violation -> {
              assertThat(violation.instancePointer()).startsWith("/schema/0");
              assertThat(violation.message()).contains("columns");
            });
  }

  @Test
  @SneakyThrows
  void givenSchemaWithDefinitionsInGroups_whenValidate_thenResolvesThemWithoutWarning() {
    var json = json("{ \"server\": { \"path\": 5 } }");
    var logger = (Logger) LogManager.getLogger(UnknownKeywordFactory.class);
    var level = logger.getLevel();
    var warnings = new CopyOnWriteArrayList<String>();
    var appender =
        new AbstractAppender("warnings", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            warnings.add(event.getMessage().getFormattedMessage());
          }
        };
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.WARN);

    try {
      assertThat(JsonSchemaValidator.validate(json, GROUPED_DEFINITIONS_SCHEMA))
          .extracting(Violation::instancePointer)
          .containsExactly("/server/path");
    } finally {
      logger.setLevel(level);
      logger.removeAppender(appender);
      appender.stop();
    }

    assertThat(warnings).isEmpty();
  }

  @SneakyThrows
  private static JsonNode json(String content) {
    return MAPPER.readTree(content);
  }
}
