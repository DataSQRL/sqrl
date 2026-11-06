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
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.error.ErrorMessage.Severity;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.hint.DocStringParser;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlComments;
import com.datasqrl.planner.tags.TagDefinition.RequiredColumn;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class TagAnalyzerTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(TagAnalyzerTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelOptCluster CLUSTER =
      RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(TYPES));

  private static final RelDataType STRING =
      TYPES.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);

  private static final RelDataType INT = TYPES.createSqlType(SqlTypeName.INTEGER);

  private static final TagDefinitions DEFINITIONS =
      TagDefinitions.of(
          List.of(
              TagDefinition.of(
                  "classification", List.of("unclassified", "secret", "top_secret"), List.of()),
              TagDefinition.of("pii", List.of("masked", "raw"), List.of()),
              TagDefinition.of("gdpr", List.of(), List.of()),
              TagDefinition.of(
                  "jurisdiction",
                  List.of(),
                  List.of(new RequiredColumn("jurisdiction", "STRING", STRING)))));

  private final TagAnalyzer analyzer = new TagAnalyzer(DEFINITIONS);
  private final ErrorCollector errors = ErrorCollector.root();

  @Test
  void givenSeveralInputs_whenAnalyze_thenInheritsUnionWithHighestValue() {
    var table =
        table("MyTable")
            .fromTables(
                List.of(
                    input("Customers", "@classification=secret @pii=raw"),
                    input("Orders", "@classification=top_secret @gdpr")))
            .build();

    var tags = analyzer.analyze(table, List.of(), false, errors);

    assertThat(tags.toValueMap())
        .containsExactly(
            entry("classification", "top_secret"), entry("gdpr", "true"), entry("pii", "raw"));
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenHigherDeclaredTag_whenAnalyze_thenRaisesValue() {
    var table =
        table("MyTable")
            .fromTables(List.of(input("Customers", "@classification=secret @pii=masked")))
            .documentation(DocStringParser.parse("Customers @classification=top_secret @pii=raw"))
            .build();

    var tags = analyzer.analyze(table, List.of(), true, errors);

    assertThat(tags).hasToString("@classification=top_secret @pii=raw");
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenTagsInColumnDocsAndComments_whenAnalyze_thenTheyAreDeclared() {
    var table =
        table("MyTable")
            .documentation(
                DocStringParser.parse("Customers\n# Columns:\n- id: The id @classification=secret"))
            .build();

    var tags = analyzer.analyze(table, List.of("Contact email @pii=raw"), false, errors);

    assertThat(tags).hasToString("@classification=secret @pii=raw");
  }

  @Test
  void givenMissingAndLowerTags_whenAnalyzeTableThatMustDeclare_thenReportsAllViolations() {
    var table =
        table("MyTable")
            .fromTables(
                List.of(
                    input("Customers", "@classification=secret @pii=raw"),
                    input("Orders", "@classification=secret")))
            .documentation(DocStringParser.parse("Lower @classification=unclassified"))
            .build();

    var tags = analyzer.analyze(table, List.of(), true, errors);

    assertThat(messages())
        .containsExactly(
            "Table [MyTable] declares tag [@classification=unclassified] which is lower than"
                + " [@classification=secret] inherited from [Customers, Orders]: declare it in the"
                + " doc string or declassify it with /*+declassify(classification) */",
            "Table [MyTable] must declare tag [@pii=raw] inherited from [Customers]: declare it in"
                + " the doc string or declassify it with /*+declassify(pii) */");
    assertThat(tags).hasToString("@classification=secret @pii=raw");
  }

  @Test
  void givenTableThatNeedNotDeclare_whenAnalyze_thenCarriesInheritedTagsWithoutErrors() {
    var table = table("_Hidden").fromTables(List.of(input("Customers", "@pii=raw"))).build();

    var tags = analyzer.analyze(table, List.of(), false, errors);

    assertThat(tags).hasToString("@pii=raw");
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenDeclassifyHint_whenAnalyze_thenTakesDeclaredValueOrDropsTag() {
    var table =
        table("MyTable")
            .fromTables(List.of(input("Customers", "@classification=top_secret @pii=raw @gdpr")))
            .hints(hints("/*+declassify(PII, classification) */"))
            .documentation(DocStringParser.parse("Counts @classification=secret @gdpr"))
            .build();

    var tags = analyzer.analyze(table, List.of(), true, errors);

    assertThat(tags).hasToString("@classification=secret @gdpr");
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenDeclassifyHintWithUndefinedOrNotInheritedTags_whenAnalyze_thenReportsThem() {
    var table =
        table("MyTable")
            .fromTables(List.of(input("Customers", "@pii=raw")))
            .hints(hints("/*+declassify(pii, gdpr, nope) */"))
            .build();

    analyzer.analyze(table, List.of(), true, errors);

    assertThat(all())
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
  void givenTagWithRequiredColumns_whenAnalyze_thenChecksTheColumns() {
    List<TableOrFunctionAnalysis> inputs = List.of(input("Customers", "@jurisdiction"));

    analyzer.analyze(
        table("Missing", "id", INT).fromTables(inputs).build(), List.of(), false, errors);
    analyzer.analyze(
        table("WrongType", "JURISDICTION", INT).fromTables(inputs).build(),
        List.of(),
        false,
        errors);
    analyzer.analyze(
        table("Valid", "Jurisdiction", TYPES.createSqlType(SqlTypeName.VARCHAR, 3))
            .fromTables(inputs)
            .build(),
        List.of(),
        false,
        errors);

    assertThat(messages())
        .containsExactly(
            "Table [Missing] has tag [@jurisdiction] which requires column [jurisdiction] of type"
                + " [STRING], but the column is missing",
            "Table [WrongType] has tag [@jurisdiction] which requires column [jurisdiction] of type"
                + " [STRING], but the column has type [INT NOT NULL]");
  }

  @Test
  void givenDeclassifiedTagWithRequiredColumns_whenAnalyze_thenColumnsAreNotRequired() {
    var table =
        table("MyTable")
            .fromTables(List.of(input("Customers", "@jurisdiction")))
            .hints(hints("/*+declassify(jurisdiction) */"))
            .build();

    var tags = analyzer.analyze(table, List.of(), true, errors);

    assertThat(tags.isEmpty()).isTrue();
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenDataWithUndeclaredTags_whenCheckWrite_thenReportsTheTags() {
    var target = table("Copy").tags(DEFINITIONS.parse("@pii=masked", invalid -> {})).build();
    var inputs = List.of(input("Customers", "@pii=raw @gdpr"));

    analyzer.checkWrite(target, inputs, errors);

    assertThat(messages())
        .containsExactly(
            "Table [Copy] must declare tag [@gdpr] inherited from [Customers]: declare it in the"
                + " doc string of the table",
            "Table [Copy] declares tag [@pii=masked] which is lower than [@pii=raw] inherited from"
                + " [Customers]: declare it in the doc string of the table");
  }

  @Test
  void givenDisabledTags_whenAnalyze_thenNoTags() {
    var table =
        table("MyTable")
            .documentation(DocStringParser.parse("Customers @pii=raw @undefined"))
            .hints(hints("/*+declassify(pii) */"))
            .build();

    var tags = new TagAnalyzer(TagDefinitions.DISABLED).analyze(table, List.of(), true, errors);

    assertThat(tags.isEmpty()).isTrue();
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  private static TableAnalysis.TableAnalysisBuilder table(String name) {
    return table(name, "id", INT);
  }

  private static TableAnalysis.TableAnalysisBuilder table(
      String name, String column, RelDataType type) {
    var values = LogicalValues.createEmpty(CLUSTER, TYPES.builder().add(column, type).build());
    return TableAnalysis.builder()
        .objectIdentifier(ObjectIdentifier.of("default_catalog", "default_database", name))
        .collapsedRelnode(values);
  }

  private static TableAnalysis input(String name, String tags) {
    return table(name).tags(DEFINITIONS.parse(tags, invalid -> {})).build();
  }

  private static PlannerHints hints(String hints) {
    return PlannerHints.from(
        SqrlComments.parse(new ParsedObject<>(hints, FileLocation.START)),
        Optional.empty(),
        ErrorCollector.root());
  }

  private List<ErrorMessage> all() {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }

  private List<String> messages() {
    return all().stream().map(ErrorMessage::getMessage).toList();
  }
}
