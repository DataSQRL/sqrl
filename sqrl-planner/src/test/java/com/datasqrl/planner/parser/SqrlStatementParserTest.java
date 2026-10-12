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
}
