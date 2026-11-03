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
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.error.ErrorMessage.Severity;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.tags.TagDefinition.RequiredColumn;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class TagAnalyzerTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(TagAnalyzerTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelDataType STRING =
      TYPES.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);

  private static final TagDefinitions DEFS =
      TagDefinitions.of(
          List.of(
              new TagDefinition(
                  "classification", List.of("unclassified", "secret", "top_secret"), List.of()),
              new TagDefinition("pii", List.of("masked", "raw"), List.of()),
              new TagDefinition("gdpr", List.of(), List.of()),
              new TagDefinition(
                  "jurisdiction",
                  List.of(),
                  List.of(new RequiredColumn("jurisdiction", "STRING", STRING)))));

  @Test
  void givenSeveralInputs_whenCompute_thenInheritsUnionWithMaximum() {
    var customers = table("Customers", "@classification=secret @pii=raw");
    var orders = table("Orders", "@classification=top_secret @gdpr");

    var result = TagAnalyzer.compute(List.of(customers, orders), TableTags.EMPTY, Set.of());

    assertThat(result.inherited().toString())
        .isEqualTo("classification=top_secret, gdpr=true, pii=raw");
    assertThat(result.effective()).isEqualTo(result.inherited());
  }

  @Test
  void givenNoInputs_whenCompute_thenEffectiveIsDeclared() {
    var declared = tags("@pii=masked");

    var result = TagAnalyzer.compute(List.of(), declared, Set.of());

    assertThat(result.inherited().isEmpty()).isTrue();
    assertThat(result.effective().toString()).isEqualTo("pii=masked");
  }

  @Test
  void givenHigherDeclaredTag_whenCompute_thenRaisesValue() {
    var result =
        TagAnalyzer.compute(
            List.of(table("Customers", "@classification=unclassified")),
            tags("@classification=secret"),
            Set.of());

    assertThat(result.effective().toString()).isEqualTo("classification=secret");
  }

  @Test
  void givenDeclassifiedTags_whenCompute_thenLowersOrRemoves() {
    var input = table("Customers", "@classification=top_secret @pii=raw @gdpr");

    var result =
        TagAnalyzer.compute(
            List.of(input), tags("@classification=secret @gdpr"), Set.of("classification", "pii"));

    assertThat(result.effective().toString()).isEqualTo("classification=secret, gdpr=true");
  }

  @Test
  void givenInvalidTagsOnInput_whenCompute_thenInvalidTagsAreNotInherited() {
    var input = table("Customers", "@pii=raw @unknown");

    var result = TagAnalyzer.compute(List.of(input), tags("@pii=raw @other"), Set.of());

    assertThat(result.inherited().hasInvalidTags()).isFalse();
    assertThat(result.effective().hasInvalidTags()).isFalse();
  }

  @Test
  void givenDeclaredInheritedTags_whenValidate_thenNoErrors() {
    var inputs = List.of(table("Customers", "@pii=raw @classification=secret"));
    var declared = tags("@pii=raw @classification=top_secret");

    var errors = validate(inputs, declared, Set.of(), true, null);

    assertThat(messages(errors)).isEmpty();
  }

  @Test
  void givenValidPublicTable_whenCompute_thenEffectiveEqualsDeclared() {
    var inputs = List.of(table("Customers", "@pii=raw @gdpr"));
    var declared = tags("@pii=raw @gdpr @classification=secret");

    var result = TagAnalyzer.compute(inputs, declared, Set.of());

    assertThat(result.effective()).isEqualTo(declared);
  }

  @Test
  void givenMissingAndLowerTags_whenValidate_thenReportsAllViolations() {
    var inputs =
        List.of(
            table("Customers", "@pii=raw @classification=secret"),
            table("Orders", "@pii=raw @gdpr"));
    var declared = tags("@classification=unclassified @unknown");

    var errors = validate(inputs, declared, Set.of(), true, null);

    assertThat(messages(errors))
        .containsExactly(
            "Invalid tag on table [MyTable]: Tag [@unknown] is not defined in compiler.tags",
            "Table [MyTable] declares tag [@classification=unclassified] which is lower than"
                + " [@classification=secret] inherited from [Customers]: raise it or declassify it"
                + " with /*+declassify(classification) */",
            "Table [MyTable] must declare tag [@gdpr] inherited from [Orders]: add it to the doc"
                + " string or declassify it with /*+declassify(gdpr) */",
            "Table [MyTable] must declare tag [@pii=raw] inherited from [Customers, Orders]: add it"
                + " to the doc string or declassify it with /*+declassify(pii) */");
    assertThat(errors.hasErrors()).isTrue();
  }

  @Test
  void givenHiddenTable_whenValidate_thenInheritedTagsAreNotRequired() {
    var inputs = List.of(table("Customers", "@pii=raw"));

    var errors = validate(inputs, TableTags.EMPTY, Set.of(), false, null);

    assertThat(messages(errors)).isEmpty();
  }

  @Test
  void givenDeclassifiedTags_whenValidate_thenNotRequiredAndChecked() {
    var inputs = List.of(table("Customers", "@pii=raw"));

    var errors = validate(inputs, TableTags.EMPTY, Set.of("pii", "gdpr", "nope"), true, null);

    assertThat(all(errors))
        .extracting(ErrorMessage::getSeverity, ErrorMessage::getMessage)
        .containsExactly(
            tuple(
                Severity.WARN,
                "Table [MyTable] declassifies tag [gdpr] which it does not inherit from its input"
                    + " tables"),
            tuple(
                Severity.FATAL,
                "Table [MyTable] declassifies tag [nope] which is not defined in compiler.tags"));
  }

  @Test
  void givenTagWithRequiredColumns_whenValidate_thenChecksColumns() {
    var inputs = List.of(table("Customers", "@jurisdiction"));
    var missingColumn = rowType("id", TYPES.createSqlType(SqlTypeName.INTEGER));
    var wrongType = rowType("JURISDICTION", TYPES.createSqlType(SqlTypeName.INTEGER));
    var validType = rowType("Jurisdiction", TYPES.createSqlType(SqlTypeName.VARCHAR, 3));

    // hidden tables are checked for required columns too
    assertThat(messages(validate(inputs, TableTags.EMPTY, Set.of(), false, missingColumn)))
        .containsExactly(
            "Table [MyTable] has tag [@jurisdiction] which requires column [jurisdiction] of type"
                + " [STRING], but the column is missing");
    assertThat(messages(validate(inputs, TableTags.EMPTY, Set.of(), false, wrongType)))
        .containsExactly(
            "Table [MyTable] has tag [@jurisdiction] which requires column [jurisdiction] of type"
                + " [STRING], but the column has type [INT NOT NULL]");
    assertThat(messages(validate(inputs, TableTags.EMPTY, Set.of(), false, validType))).isEmpty();
  }

  @Test
  void givenDeclassifiedTagWithRequiredColumns_whenValidate_thenNoColumnCheck() {
    var inputs = List.of(table("Customers", "@jurisdiction"));
    var noColumns = rowType("id", TYPES.createSqlType(SqlTypeName.INTEGER));

    var errors = validate(inputs, TableTags.EMPTY, Set.of("jurisdiction"), true, noColumns);

    assertThat(messages(errors)).isEmpty();
  }

  private static ErrorCollector validate(
      List<? extends TableOrFunctionAnalysis> inputs,
      TableTags declared,
      Set<String> declassified,
      boolean validateDeclared,
      RelDataType rowType) {
    var errors = ErrorCollector.root();
    var result = TagAnalyzer.compute(inputs, declared, declassified);
    TagAnalyzer.validate(
        "MyTable", DEFS, result, declared, declassified, inputs, rowType, validateDeclared, errors);
    return errors;
  }

  private static TableTags tags(String doc) {
    var builder = TableTags.builder(DEFS);
    for (var token : doc.trim().split("\\s+")) {
      var parts = token.substring(1).split("=", 2);
      builder.add(parts[0], parts.length > 1 ? parts[1] : null);
    }
    return builder.build();
  }

  private static TableAnalysis table(String name, String tags) {
    return TableAnalysis.builder()
        .objectIdentifier(ObjectIdentifier.of("default_catalog", "default_database", name))
        .tags(tags(tags))
        .build();
  }

  private static RelDataType rowType(String column, RelDataType type) {
    return TYPES.builder().add(column, type).build();
  }

  private static List<ErrorMessage> all(ErrorCollector errors) {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }

  private static List<String> messages(ErrorCollector errors) {
    return all(errors).stream().map(ErrorMessage::getMessage).toList();
  }
}
