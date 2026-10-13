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
package com.datasqrl.lsp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.nio.file.Path;
import java.util.List;
import org.eclipse.lsp4j.DocumentSymbol;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.SymbolKind;
import org.junit.jupiter.api.Test;

class DocumentOutlineTest {

  private static final String SCRIPT =
      """
      IMPORT shop.sources AS src;

      CREATE TABLE Events (id BIGINT, ts TIMESTAMP_LTZ(3)) WITH ('connector' = 'datagen');

      /** Recent events */
      Recent := SELECT * FROM Events;
      Recent.big := id > 10;
      Recent.events := SELECT * FROM Events e WHERE e.id = this.id;
      ById(id BIGINT) := SELECT * FROM Events WHERE id = :id;
      /*+test */
      Deduped := DISTINCT Events ON id ORDER BY ts DESC;

      EXPORT Recent TO print.recent;
      """;

  @Test
  void givenScript_whenOutlined_thenListsItsStatementsWithTheirNames() {
    var outline = DocumentOutline.of(SCRIPT, Path.of("/project/main.sqrl"), List.of());

    assertThat(outline)
        .extracting(
            DocumentSymbol::getName,
            DocumentSymbol::getKind,
            DocumentSymbol::getDetail,
            DocumentSymbol::getSelectionRange)
        .containsExactly(
            tuple("IMPORT shop.sources", SymbolKind.Module, "as src", range(0, 7, 0, 19)),
            tuple("Events", SymbolKind.Struct, null, range(2, 13, 2, 19)),
            tuple("Recent", SymbolKind.Class, "", range(5, 0, 5, 6)),
            tuple("ById", SymbolKind.Function, "(id BIGINT)", range(8, 0, 8, 4)),
            tuple("Deduped", SymbolKind.Class, "test", range(10, 0, 10, 7)),
            tuple("EXPORT Recent", SymbolKind.Event, "to print.recent", range(12, 7, 12, 13)));
  }

  @Test
  void givenColumnAndRelationship_whenOutlined_thenNestsThemInTheirTable() {
    var outline = DocumentOutline.of(SCRIPT, Path.of("/project/main.sqrl"), List.of());

    var recent = outline.get(2);
    assertThat(recent.getRange()).isEqualTo(range(4, 0, 7, 61));
    assertThat(recent.getChildren())
        .extracting(
            DocumentSymbol::getName, DocumentSymbol::getKind, DocumentSymbol::getSelectionRange)
        .containsExactly(
            tuple("big", SymbolKind.Field, range(6, 0, 6, 10)),
            tuple("events", SymbolKind.Property, range(7, 0, 7, 13)));
  }

  @Test
  void givenRelationshipAfterOtherTables_whenOutlined_thenKeepsTheRangeOfItsTable() {
    var script =
        "A := SELECT 1 AS id;\nB := SELECT 2 AS id;\nA.b := SELECT * FROM B WHERE id = this.id;\n";

    var outline = DocumentOutline.of(script, Path.of("/project/main.sqrl"), List.of());

    assertThat(outline).extracting(DocumentSymbol::getName).containsExactly("A", "B");
    assertThat(outline.get(0).getRange()).isEqualTo(range(0, 0, 0, 20));
    assertThat(outline.get(0).getChildren())
        .extracting(DocumentSymbol::getName, DocumentSymbol::getSelectionRange)
        .containsExactly(tuple("b", range(2, 0, 2, 3)));
  }

  @Test
  void givenStatementThatDoesNotParse_whenOutlined_thenListsTheOthers() {
    var script = "A := SELECT 1;\nB := ;\nIMPORT ;\nC := SELECT 2;\n";

    var outline = DocumentOutline.of(script, Path.of("/project/main.sqrl"), List.of());

    assertThat(outline).extracting(DocumentSymbol::getName).containsExactly("A", "C");
  }

  @Test
  void givenEmptyScript_whenOutlined_thenIsEmpty() {
    assertThat(DocumentOutline.of("  \n", Path.of("/project/main.sqrl"), List.of())).isEmpty();
  }

  private static Range range(int startLine, int startChar, int endLine, int endChar) {
    return new Range(new Position(startLine, startChar), new Position(endLine, endChar));
  }
}
