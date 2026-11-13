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
package com.datasqrl.planner.tags;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.config.PackageJson.TagConfig;
import com.datasqrl.error.ErrorCollector;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class TagDefinitionsTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(TagDefinitionsTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelDataType STRING =
      TYPES.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);

  private static final TagDefinitions DEFINITIONS =
      TagDefinitions.of(
          List.of(
              TagDefinition.of("classification", List.of("secret", "top_secret"), List.of()),
              TagDefinition.of("gdpr", List.of(), List.of())));

  private final ErrorCollector errors = ErrorCollector.root();

  @Test
  void givenNoConfig_whenFrom_thenDisabled() {
    var definitions = TagDefinitions.from(Optional.empty(), this::parseType, errors);

    assertThat(definitions.isEnabled()).isFalse();
    assertThat(definitions.parse("@pii=raw @undefined", invalid -> {}).isEmpty()).isTrue();
  }

  @Test
  void givenConfig_whenFrom_thenDefinesNormalizedTags() {
    var configs = new LinkedHashMap<String, TagConfig>();
    configs.put("Classification", config(List.of("Secret", "TOP_SECRET"), Map.of()));
    configs.put("jurisdiction", config(List.of(), Map.of("jurisdiction", "STRING")));

    var definitions = TagDefinitions.from(Optional.of(configs), this::parseType, errors);

    assertThat(errors.hasErrors()).isFalse();
    assertThat(definitions.get("CLASSIFICATION").orElseThrow())
        .satisfies(
            tag -> {
              assertThat(tag.name()).isEqualTo("classification");
              assertThat(tag.values()).containsExactly("secret", "top_secret");
              assertThat(tag.isBoolean()).isFalse();
            });
    assertThat(definitions.get("jurisdiction").orElseThrow())
        .satisfies(
            tag -> {
              assertThat(tag.values()).containsExactly(TagDefinition.TRUE);
              assertThat(tag.isBoolean()).isTrue();
              assertThat(tag.requiredColumns())
                  .singleElement()
                  .satisfies(column -> assertThat(column.type()).isEqualTo(STRING));
            });
  }

  @Test
  void givenDuplicatesDifferingInCaseAndInvalidType_whenFrom_thenReportsAllErrors() {
    var configs = new LinkedHashMap<String, TagConfig>();
    configs.put("pii", config(List.of("raw", "RAW"), Map.of("id", "NOT A TYPE")));
    configs.put("PII", config(List.of(), Map.of()));

    var definitions = TagDefinitions.from(Optional.of(configs), this::parseType, errors);

    assertThat(messages())
        .containsExactly(
            "Duplicate value [RAW] of tag [pii] in compiler.tags: values are case-insensitive",
            "Invalid type [NOT A TYPE] of required column [id] of tag [pii] in compiler.tags:"
                + " Unknown type",
            "Duplicate tag [pii] in compiler.tags: tag names are case-insensitive");
    assertThat(definitions.get("pii").orElseThrow().values()).containsExactly("raw");
  }

  @Test
  void givenRequiredColumnsDifferingInCase_whenFrom_thenReportsTheDuplicate() {
    var columns = new LinkedHashMap<String, String>();
    columns.put("jurisdiction", "STRING");
    columns.put("JURISDICTION", "STRING");

    var definitions =
        TagDefinitions.from(
            Optional.of(Map.of("jurisdiction", config(List.of(), columns))),
            this::parseType,
            errors);

    assertThat(messages())
        .containsExactly(
            "Duplicate required column [JURISDICTION] of tag [jurisdiction] in compiler.tags:"
                + " column names are case-insensitive");
    assertThat(definitions.get("jurisdiction").orElseThrow().requiredColumns())
        .extracting(TagDefinition.RequiredColumn::name)
        .containsExactly("jurisdiction");
  }

  @Test
  void givenTextWithTags_whenParse_thenResolvesTagsCaseInsensitively() {
    var tags =
        DEFINITIONS.parse(
            "Customers @CLASSIFICATION=secret, contact user@example.com @gdpr.\n"
                + "@classification=Top_Secret",
            invalid -> {});

    assertThat(tags).hasToString("@classification=top_secret @gdpr");
  }

  @Test
  void givenInvalidTags_whenParse_thenReportsEachAndIgnoresIt() {
    var invalid = new ArrayList<String>();

    var tags =
        DEFINITIONS.parse(
            "@undefined @classification=public @classification @gdpr=yes @gdpr=true", invalid::add);

    assertThat(invalid)
        .containsExactly(
            "Tag [@undefined] is not defined in compiler.tags",
            "Tag [@classification=public] has an invalid value: allowed values are [secret,"
                + " top_secret]",
            "Tag [@classification] requires a value: allowed values are [secret, top_secret]",
            "Tag [@gdpr=yes] has an invalid value: allowed values are [true]");
    assertThat(tags).hasToString("@gdpr");
  }

  private RelDataType parseType(String type) {
    if (!type.equals("STRING")) {
      throw new IllegalArgumentException("Unknown type");
    }
    return STRING;
  }

  private static TagConfig config(List<String> values, Map<String, String> requiredColumns) {
    return new TagConfig() {
      @Override
      public List<String> getValues() {
        return values;
      }

      @Override
      public Map<String, String> getRequiredColumns() {
        return requiredColumns;
      }
    };
  }

  private List<String> messages() {
    var result = new ArrayList<String>();
    errors.forEach(message -> result.add(message.getMessage()));
    return result;
  }
}
