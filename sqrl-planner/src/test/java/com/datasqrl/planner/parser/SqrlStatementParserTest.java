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
package com.datasqrl.planner.parser;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SqrlStatementParserTest {

  private final SqrlStatementParser parser = new SqrlStatementParser();

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ExportableDatapointsChanges := SELECT * FROM Datapoints;",
        "Exports := SELECT * FROM Datapoints;",
        "Importers := SELECT * FROM Datapoints;",
        "export_table := SELECT * FROM Datapoints;",
        "IMPORTANT := SELECT * FROM Datapoints;"
      })
  void givenTableNameStartingWithImportOrExport_whenParseStatement_thenIsTableDefinition(
      String statement) {
    var parsed = parser.parseStatement(statement);

    assertThat(parsed).isInstanceOf(SqrlTableDefinition.class);
  }

  @Test
  void givenImportStatement_whenParseStatement_thenIsImport() {
    var parsed = parser.parseStatement("IMPORT mypkg.Datapoints AS Datapoints;");

    assertThat(parsed).isInstanceOf(SqrlImportStatement.class);
  }

  @Test
  void givenExportStatement_whenParseStatement_thenIsExport() {
    var parsed = parser.parseStatement("EXPORT Datapoints TO mypkg.sink;");

    assertThat(parsed).isInstanceOf(SqrlExportStatement.class);
  }

  @Test
  void givenExportStatementWithNewline_whenParseStatement_thenIsExport() {
    var parsed = parser.parseStatement("EXPORT\n  Datapoints TO mypkg.sink;");

    assertThat(parsed).isInstanceOf(SqrlExportStatement.class);
  }

  @Test
  void givenDefinitionsMissingDelimiter_whenParseCreateTables_thenTablesAreParsed() {
    var script =
        """
        CREATE TABLE Orders (id INT) WITH ('connector' = 'datagen');
        A := SELECT 1
        B := SELECT 2;
        """;
    var errors = ErrorCollector.root();

    var tables = parser.parseCreateTables(script, errors);

    assertThat(tables)
        .singleElement()
        .satisfies(
            table ->
                assertThat(table.statement().get()).isInstanceOf(SqrlCreateTableStatement.class));
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenDefinitionThatFailsToParse_whenParseScript_thenItsSourceIsKept() {
    var script =
        """
        A := SELECT 1;
        B := DISTINCT A ON id;
        C := SELECT 2;
        """;

    var parsed = parser.parseScript(script, ErrorCollector.root().withScript("test.sqrl", script));

    assertThat(parsed.statements()).hasSize(2);
    assertThat(parsed.unparsedStatements())
        .singleElement()
        .satisfies(
            statement -> {
              assertThat(statement.get()).startsWith("B := DISTINCT A ON id;");
              assertThat(statement.getFileLocation()).isEqualTo(new FileLocation(2, 1));
            });
    assertThat(parsed.firstError()).isPresent();
  }
}
