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

import com.datasqrl.compile.check.CheckRequest;
import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.compile.check.ScriptChecker;
import com.datasqrl.compile.check.SymbolIndex.Reference;
import com.datasqrl.lsp.SymbolLookup.ColumnTarget;
import com.datasqrl.lsp.SymbolLookup.DatabaseTarget;
import com.datasqrl.lsp.SymbolLookup.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.TextEdit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Checks a project with a nested JSON source, a script imported as a database with an alias, and
 * pass-through functions, and finds what their names refer to.
 */
class IndexedNamesTest {

  private static final String SCRIPT =
      """
      IMPORT sources AS src;
      Cities := SELECT c.id, c.address.city AS city, address.geo.lat AS lat FROM src.Customers c;
      Latest := DISTINCT src.Customers ON id ORDER BY updated_at DESC;
      PassThroughTable RETURNS (id BIGINT NOT NULL, name STRING NOT NULL) :=
          SELECT c.id, name FROM "Latest" c WHERE c.address IS NOT NULL;
      Latest.others RETURNS (id BIGINT NOT NULL) :=
          WITH x AS (SELECT l.id FROM "Latest" l WHERE l.id <> this.id) SELECT x.id FROM x;
      Latest.cities := SELECT * FROM Cities c WHERE c.id = this.id;
      """;

  private static final List<String> LINES = SCRIPT.lines().toList();

  @TempDir static Path dir;

  private static Path main;
  private static Path sources;
  private static CheckResult result;

  @BeforeAll
  static void check() throws Exception {
    Files.writeString(
        dir.resolve("package.json"),
        """
        {"version": "1", "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
         "script": {"main": "main.sqrl"}}
        """);
    Files.writeString(
        dir.resolve("customers.jsonl"),
        """
        {"id": 1, "name": "a", "address": {"city": "X", "geo": {"lat": 1.5, "lon": 2.5}},\
         "updated_at": "2024-01-01T10:00:00.000Z"}
        """);
    sources = dir.resolve("sources.sqrl").toAbsolutePath().normalize();
    Files.writeString(
        sources,
        """
        CREATE TABLE Customers (
          PRIMARY KEY (id, `updated_at`) NOT ENFORCED,
          WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECOND
        ) LIKE `customers.jsonl`;
        """);
    main = dir.resolve("main.sqrl").toAbsolutePath().normalize();
    Files.writeString(main, SCRIPT);
    result =
        new ScriptChecker().check(CheckRequest.of(dir, dir.resolve("work")).withKeepCatalog(true));
    assertThat(result.hasErrors()).isFalse();
  }

  @AfterAll
  static void close() {
    result.catalog().ifPresent(catalog -> catalog.close());
  }

  @Test
  void givenNestedFields_whenLookup_thenFindsTheFieldOrColumnAtThePosition() {
    var line = LINES.get(1);

    assertThat(column(1, line.indexOf("city AS")))
        .extracting(r -> r.column(), r -> r.type().orElseThrow())
        .containsExactly(List.of("address", "city"), "STRING NOT NULL");
    assertThat(column(1, line.indexOf("address.city")))
        .satisfies(
            r -> {
              assertThat(r.column()).containsExactly("address");
              assertThat(r.type().orElseThrow()).startsWith("ROW<`city` STRING NOT NULL");
            });
    // Without the alias of the table
    assertThat(column(1, line.indexOf("lat AS")))
        .extracting(r -> r.column(), r -> r.type().orElseThrow())
        .containsExactly(List.of("address", "geo", "lat"), "DOUBLE NOT NULL");
    assertThat(column(1, line.indexOf("geo.lat")).column()).containsExactly("address", "geo");
    var hover = HoverContent.render(find(SCRIPT, 1, line.indexOf("geo.lat"))).orElseThrow();
    assertThat(hover).startsWith("```sql\naddress.geo ROW<`lat` DOUBLE NOT NULL");
  }

  @Test
  void givenDatabaseOfImportedScript_whenLookup_thenFindsItsTablesAndScript() {
    var qualifier = find(SCRIPT, 1, LINES.get(1).indexOf("src.Customers"));
    var alias = find(SCRIPT, 0, LINES.get(0).indexOf("src"));
    // While the script changed since its check, by the name before a `.`
    var edited = "-- a comment\n" + SCRIPT;
    var unchecked = find(edited, 2, LINES.get(1).indexOf("src.Customers"));

    for (var target : List.of(qualifier, alias, unchecked)) {
      assertThat(target)
          .isInstanceOfSatisfying(
              DatabaseTarget.class,
              database -> {
                assertThat(database.name()).isEqualTo("src");
                assertThat(database.tables()).extracting(t -> t.name()).contains("Customers");
                assertThat(database.scripts()).extracting(s -> s.file()).containsExactly(sources);
              });
    }
    assertThat(HoverContent.render(qualifier).orElseThrow())
        .startsWith("```sql\nDATABASE src\n```")
        .contains("`sources.sqrl`", "`Customers`");
  }

  @Test
  void givenDatabasesAndNestedFields_whenSemanticTokens_thenColorsThem() {
    var data = SemanticHighlighting.of(main, SCRIPT, List.of(result.symbols())).getData();
    var tokens = new ArrayList<List<Integer>>();
    var line = 0;
    var start = 0;
    for (var i = 0; i < data.size(); i += 5) {
      start = data.get(i) == 0 ? start + data.get(i + 1) : data.get(i + 1);
      line += data.get(i);
      tokens.add(List.of(line, start, data.get(i + 3), data.get(i + 4)));
    }
    var namespace = SemanticHighlighting.LEGEND.getTokenTypes().indexOf("namespace");
    var property = SemanticHighlighting.LEGEND.getTokenTypes().indexOf("property");

    assertThat(tokens)
        .contains(
            // The alias of the import, and the database of a table
            List.of(0, LINES.get(0).indexOf("src"), namespace, 1),
            List.of(1, LINES.get(1).indexOf("src.Customers"), namespace, 0),
            List.of(1, LINES.get(1).indexOf("geo.lat"), property, 0),
            // A column of a pass-through query
            List.of(4, LINES.get(4).indexOf("name"), property, 0));
  }

  @Test
  void givenPassthroughQueries_whenChecked_thenIndexesTheirTablesAndColumns() {
    var references =
        result.symbols().references().stream()
            .filter(r -> r.location().startLine() == 4 || r.location().startLine() == 6)
            .map(
                r ->
                    tuple(
                        LINES
                            .get(r.location().startLine())
                            .substring(r.location().startCharacter(), r.location().endCharacter()),
                        r.table().getObjectName()))
            .toList();

    // Not the common table `x`
    assertThat(references)
        .containsExactlyInAnyOrder(
            tuple("\"Latest\"", "Latest"),
            tuple("id", "Latest"),
            tuple("name", "Latest"),
            tuple("address", "Latest"),
            tuple("\"Latest\"", "Latest"),
            tuple("id", "Latest"),
            tuple("id", "Latest"),
            tuple("id", "Latest"));
    assertThat(find(SCRIPT, 4, LINES.get(4).indexOf("name")))
        .isInstanceOfSatisfying(
            ColumnTarget.class,
            c -> assertThat(c.table().orElseThrow().name()).isEqualTo("Latest"));
  }

  @Test
  void givenParentColumnOfRelationship_whenLookup_thenFindsTheColumnOfTheParentTable() {
    var line = LINES.get(7);

    var target = find(SCRIPT, 7, line.indexOf("this.id") + "this.".length());

    assertThat(target)
        .isInstanceOfSatisfying(
            ColumnTarget.class,
            column -> {
              assertThat(column.table().orElseThrow().name()).isEqualTo("Latest");
              assertThat(column.reference().column()).containsExactly("id");
              assertThat(column.reference().location().startCharacter())
                  .isEqualTo(line.indexOf("this.id") + "this.".length());
            });
    assertThat(HoverContent.render(target).orElseThrow())
        .startsWith("```sql\nid BIGINT NOT NULL\n```")
        .contains("Column of `Latest`");
  }

  @Test
  void givenQualifiedNames_whenCompleted_thenSuggestsNestedFieldsAndPassthroughColumns() {
    // In a GROUP BY, where the compiler suggests nothing
    var grouped = "X := SELECT id FROM src.Customers c GROUP BY c.address.geo.";
    assertThat(labels(complete(grouped))).containsExactly("lat", "lon");
    var unqualified = "X := SELECT id FROM src.Customers GROUP BY address.";
    assertThat(labels(complete(unqualified))).containsExactly("city", "geo");
    // Only columns follow a qualifier
    var passthrough = "P RETURNS (id BIGINT) := SELECT r.";
    assertThat(labels(complete(passthrough + " FROM \"Latest\" r", passthrough.length())))
        .containsExactly("id", "name", "address", "updated_at");
  }

  @Test
  void givenTableInPassthroughQuery_whenRenamed_thenKeepsItsQuotes() throws Exception {
    var rename = new SymbolRename(Map.of(dir, result.symbols()), file -> read(file));
    var target = rename.prepare(main, SCRIPT, 2, 0);

    var edits =
        rename
            .plan(target, "Newest")
            .toWorkspaceEdit(file -> file.toString(), false)
            .getChanges()
            .get(main.toString());

    assertThat(edits)
        .extracting(e -> e.getRange().getStart().getLine(), TextEdit::getNewText)
        .contains(tuple(2, "Newest"), tuple(4, "\"Newest\""), tuple(5, "Newest"));
  }

  private static Optional<String> read(Path file) {
    try {
      return Optional.of(Files.readString(file));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private static Target find(String text, int line, int character) {
    return SymbolLookup.find(List.of(result.symbols()), main, text, line, character).orElseThrow();
  }

  private static Reference column(int line, int character) {
    var target = find(SCRIPT, line, character);
    assertThat(target).isInstanceOf(ColumnTarget.class);
    return ((ColumnTarget) target).reference();
  }

  private static List<CompletionItem> complete(String statement) {
    return complete(statement, statement.length());
  }

  private static List<CompletionItem> complete(String statement, int cursor) {
    var text = SCRIPT + statement;
    return new ScriptCompletion(
            main,
            result.catalog(),
            List.of(result.symbols()),
            Optional.of(dir.resolve("work").resolve("build")))
        .complete(CompletionContext.at(text, SCRIPT.length() + cursor));
  }

  private static List<String> labels(List<CompletionItem> items) {
    return items.stream().map(CompletionItem::getLabel).toList();
  }
}
