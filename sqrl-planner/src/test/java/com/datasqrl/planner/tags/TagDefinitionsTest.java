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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.config.PackageJson.TagConfig;
import com.datasqrl.config.TagConfigImpl;
import com.datasqrl.config.TagConfigImpl.RequiredColumnConfigImpl;
import com.datasqrl.error.ErrorCollector;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class TagDefinitionsTest {

  private static final JavaTypeFactoryImpl TYPE_FACTORY = new JavaTypeFactoryImpl();

  private static final Function<String, RelDataType> TYPE_PARSER =
      type -> TYPE_FACTORY.createSqlType(SqlTypeName.valueOf(type));

  @Test
  void givenNoConfig_whenFrom_thenDisabled() {
    var defs = TagDefinitions.from(Optional.empty(), TYPE_PARSER, ErrorCollector.root());

    assertThat(defs).isSameAs(TagDefinitions.DISABLED);
    assertThat(defs.isEnabled()).isFalse();
  }

  @Test
  void givenValidConfig_whenFrom_thenNormalizedDefinitions() {
    var errors = ErrorCollector.root();

    var defs =
        TagDefinitions.from(
            configs(
                tag("Classification", "Unclassified", "SECRET"),
                tag("gdpr"),
                new TagConfigImpl(
                    "jurisdiction",
                    null,
                    List.of(new RequiredColumnConfigImpl("jurisdiction", "VARCHAR")))),
            TYPE_PARSER,
            errors);

    assertThat(errors.hasErrors()).isFalse();
    assertThat(defs.isEnabled()).isTrue();
    var classification = defs.get("CLASSIFICATION").orElseThrow();
    assertThat(classification.values()).containsExactly("unclassified", "secret");
    assertThat(classification.isBoolean()).isFalse();
    assertThat(defs.get("gdpr").orElseThrow().isBoolean()).isTrue();
    assertThat(defs.get("jurisdiction").orElseThrow().requiredColumns())
        .singleElement()
        .satisfies(
            column -> {
              assertThat(column.name()).isEqualTo("jurisdiction");
              assertThat(column.type().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
            });
    assertThat(defs.get("unknown")).isEmpty();
  }

  @Test
  void givenInvalidConfig_whenFrom_thenReportsAllErrors() {
    var errors = ErrorCollector.root();

    TagDefinitions.from(
        configs(
            tag("pii", "raw", "RAW"),
            tag("PII"),
            tag("has space"),
            tag("ok", "a=b"),
            new TagConfigImpl(
                "jurisdiction",
                null,
                List.of(
                    new RequiredColumnConfigImpl("jurisdiction", "NOT_A_TYPE"),
                    new RequiredColumnConfigImpl("Jurisdiction", "VARCHAR")))),
        TYPE_PARSER,
        errors);

    assertThat(messages(errors))
        .containsExactly(
            "Duplicate value [raw] for tag [pii] in compiler.tags",
            "Duplicate tag [pii] in compiler.tags",
            "Invalid tag name [has space] in compiler.tags: only letters, digits, underscores and"
                + " dashes are allowed",
            "Invalid value [a=b] for tag [ok] in compiler.tags: only letters, digits, underscores"
                + " and dashes are allowed",
            "Invalid type [NOT_A_TYPE] for required column [jurisdiction] of tag [jurisdiction] in"
                + " compiler.tags: No enum constant org.apache.calcite.sql.type.SqlTypeName"
                + ".NOT_A_TYPE",
            "Duplicate required column [Jurisdiction] for tag [jurisdiction] in compiler.tags");
  }

  @Test
  void givenOrderedTag_whenOffsetOf_thenReturnsIndex() {
    var def = new TagDefinition("pii", List.of("masked", "raw"), List.of());

    assertThat(def.offsetOf("masked")).hasValue(0);
    assertThat(def.offsetOf("raw")).hasValue(1);
    assertThat(def.offsetOf("true")).isEmpty();
    assertThat(def.valueAt(1)).isEqualTo("raw");
  }

  @Test
  void givenBooleanTag_whenOffsetOf_thenOnlyTrueIsValid() {
    var def = new TagDefinition("gdpr", List.of(), List.of());

    assertThat(def.offsetOf("true")).hasValue(TagDefinition.BOOLEAN_OFFSET);
    assertThat(def.offsetOf("false")).isEmpty();
    assertThat(def.valueAt(TagDefinition.BOOLEAN_OFFSET)).isEqualTo("true");
    assertThatThrownBy(() -> def.valueAt(0)).isInstanceOf(IllegalArgumentException.class);
  }

  private static TagConfig tag(String name, String... values) {
    return new TagConfigImpl(name, List.of(values), null);
  }

  private static Optional<List<TagConfig>> configs(TagConfig... configs) {
    return Optional.of(List.of(configs));
  }

  private static List<String> messages(ErrorCollector errors) {
    var result = new ArrayList<String>();
    errors.forEach(msg -> result.add(msg.getMessage()));
    return result;
  }
}
