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

import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.SymbolIndex.ApiFile;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.lsp.GraphqlNames.Found;
import com.datasqrl.lsp.GraphqlNames.Names;
import com.datasqrl.lsp.GraphqlNames.TypeField;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GraphqlNamesTest {

  private static final Path SCHEMA_FILE = Path.of("/project/schema.graphqls");
  private static final Path OPERATIONS_FILE = Path.of("/project/queries.graphql");

  private static final String SCHEMA =
      """
      schema { query: Root, subscription: Updates }

      \"""
      An order, with a "quoted" description
      \"""
      type Orders implements Node {
        id: ID!
        # The items of the order
        items: [Item!]
      }

      type Item { id: ID! }

      type Root {
        "All orders" Orders(limit: Int): [Orders!]
        other: Int
      }

      extend type Root {
        order(id: ID!): Orders
      }

      type Updates { Orders: Orders }

      interface Node { id: ID! }
      """;

  private static final String OPERATIONS =
      """
      query Q {
        o: Orders(limit: 1) { id items { id } }
        ... on Root { Orders { id } }
      }

      subscription S { Orders { id } }

      fragment F on Orders { items { id } }
      """;

  private static final Names TABLE =
      new Names(Optional.of("orders"), Optional.of("orders"), Optional.empty());

  @Test
  void givenTable_whenFind_thenFindsRootFieldsTypeAndItsUses() {
    var found = find(TABLE, Map.of(SCHEMA_FILE, SCHEMA, OPERATIONS_FILE, OPERATIONS));

    assertThat(rename(found, SCHEMA_FILE, SCHEMA))
        .isEqualTo(
            SCHEMA
                .replace("type Orders implements", "type Purchases implements")
                .replace(
                    "\"All orders\" Orders(limit: Int): [Orders!]",
                    "\"All orders\" Purchases(limit: Int): [Purchases!]")
                .replace("order(id: ID!): Orders", "order(id: ID!): Purchases")
                .replace("{ Orders: Orders }", "{ Purchases: Purchases }"));
    assertThat(rename(found, OPERATIONS_FILE, OPERATIONS))
        .isEqualTo(
            OPERATIONS
                .replace("o: Orders(", "o: Purchases(")
                .replace("Root { Orders {", "Root { Purchases {")
                .replace("S { Orders {", "S { Purchases {")
                .replace("on Orders {", "on Purchases {"));
    assertThat(found.namedType()).contains("Orders");
    assertThat(found.hasType("item")).isTrue();
  }

  @Test
  void givenRelationship_whenFind_thenFindsTheFieldOfItsTypeWherever() {
    var relationship =
        new Names(
            Optional.empty(), Optional.empty(), Optional.of(new TypeField("ORDERS", "items")));

    var found = find(relationship, Map.of(SCHEMA_FILE, SCHEMA, OPERATIONS_FILE, OPERATIONS));

    assertThat(rename(found, SCHEMA_FILE, SCHEMA))
        .isEqualTo(SCHEMA.replace("items: [Item!]", "Purchases: [Item!]"));
    assertThat(rename(found, OPERATIONS_FILE, OPERATIONS))
        .isEqualTo(
            OPERATIONS
                .replace("id items { id }", "id Purchases { id }")
                .replace("F on Orders { items", "F on Orders { Purchases"));
  }

  @Test
  void givenOperationsWithoutSchema_whenFind_thenAssumesTheTypesOfTheGeneratedSchema() {
    var operations = "{ Orders { items { id } } }";
    var relationship =
        new Names(
            Optional.empty(), Optional.empty(), Optional.of(new TypeField("Orders", "items")));

    var found = find(relationship, Map.of(OPERATIONS_FILE, operations));

    assertThat(rename(found, OPERATIONS_FILE, operations))
        .isEqualTo("{ Orders { Purchases { id } } }");
  }

  @Test
  void givenInvalidFile_whenFind_thenReportsIt() {
    var found = find(TABLE, Map.of(SCHEMA_FILE, "type Orders {", OPERATIONS_FILE, OPERATIONS));

    assertThat(found.unreadable()).containsExactly(SCHEMA_FILE);
  }

  private static Found find(Names names, Map<Path, String> files) {
    var apiFiles =
        files.keySet().stream()
            .sorted()
            .map(file -> new ApiFile(file, file.toString().endsWith(".graphqls")))
            .toList();
    return GraphqlNames.find(names, apiFiles, file -> Optional.ofNullable(files.get(file)));
  }

  /** The text of a file with the found names replaced by {@code Purchases}. */
  private static String rename(Found found, Path file, String text) {
    var script = new ScriptText(text);
    var ranges =
        found.ranges().stream()
            .filter(range -> range.file().equals(file))
            .sorted(
                Comparator.comparingInt(
                        (SourceRange r) -> script.offset(r.startLine(), r.startCharacter()))
                    .reversed())
            .toList();
    var renamed = new StringBuilder(text);
    for (var range : ranges) {
      renamed.replace(
          script.offset(range.startLine(), range.startCharacter()),
          script.offset(range.endLine(), range.endCharacter()),
          "Purchases");
    }
    return renamed.toString();
  }
}
