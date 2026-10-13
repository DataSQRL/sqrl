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

import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.Reference;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.lsp.SemanticHighlighting.Token;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class SemanticHighlightingTest {

  @Test
  void givenTokens_whenEncoded_thenRelativeToTheirPredecessorAndWithoutOverlaps() {
    var tokens =
        List.of(
            new Token(2, 4, 3, 1, 0),
            new Token(0, 5, 6, 0, 1),
            new Token(0, 0, 4, 3, 0),
            // Overlaps the token before it
            new Token(0, 6, 2, 3, 0));

    assertThat(SemanticHighlighting.encode(tokens))
        .containsExactly(0, 0, 4, 3, 0, 0, 5, 6, 0, 1, 2, 4, 3, 1, 0);
  }

  @Test
  void givenScriptChangedSinceItsCheck_whenTokens_thenOnlyUnchangedLinesHaveThem() {
    var file = Path.of("main.sqrl");
    var checked = "Orders := SELECT * FROM src.Orders;\nTotals := SELECT o.amount FROM Orders o;\n";
    var orders = ObjectIdentifier.of("default_catalog", "default_database", "Orders");
    var index =
        new SymbolIndex(
            List.of(
                new Symbol(
                    Kind.TABLE,
                    orders,
                    "Orders",
                    new SourceRange(file, 0, 0, 0, 6),
                    "STATE",
                    List.of(),
                    List.of(),
                    List.of(),
                    Optional.empty(),
                    Map.of(),
                    false)),
            List.of(
                // The database and table of `src.Orders`, a column and a table of the script
                new Reference(
                    new SourceRange(file, 0, 24, 0, 34),
                    ObjectIdentifier.of("default_catalog", "src", "Orders"),
                    List.of(),
                    Optional.empty()),
                new Reference(
                    new SourceRange(file, 1, 19, 1, 25),
                    orders,
                    List.of("amount"),
                    Optional.of("DOUBLE")),
                new Reference(
                    new SourceRange(file, 1, 31, 1, 37), orders, List.of(), Optional.empty())),
            List.of(),
            Map.of(file, checked),
            List.of());

    assertThat(SemanticHighlighting.of(file, "-- a comment\n" + checked, List.of(index)).getData())
        .containsExactly(
            1, 0, 6, 0, 1, 0, 24, 3, 4, 0, 0, 4, 6, 0, 0, 1, 19, 6, 3, 0, 0, 12, 6, 0, 0);
    assertThat(
            SemanticHighlighting.of(
                    file, checked.replace("o.amount", "o.amount + 1"), List.of(index))
                .getData())
        .containsExactly(0, 0, 6, 0, 1, 0, 24, 3, 4, 0, 0, 4, 6, 0, 0);
  }

  @Test
  void givenEditedLines_whenMapped_thenUnchangedLinesMoveAndEditedOnesAreDropped() {
    var lines = ChangedLines.between("a\nb\nc\nd", "a\nx\ny\nc\nd");

    assertThat(lines.line(0)).isEqualTo(OptionalInt.of(0));
    assertThat(lines.line(1)).isEmpty();
    assertThat(lines.line(2)).isEqualTo(OptionalInt.of(3));
    assertThat(lines.line(3)).isEqualTo(OptionalInt.of(4));
    assertThat(lines.line(4)).isEmpty();
  }

  @Test
  void givenSameText_whenMapped_thenEveryLineStays() {
    var lines = ChangedLines.between("a\nb", "a\nb");

    assertThat(lines.line(0)).isEqualTo(OptionalInt.of(0));
    assertThat(lines.line(1)).isEqualTo(OptionalInt.of(1));
  }
}
