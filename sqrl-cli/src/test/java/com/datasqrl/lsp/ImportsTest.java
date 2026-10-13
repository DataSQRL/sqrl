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

import static com.datasqrl.lsp.TestLanguageServers.TIMEOUT_SECONDS;
import static com.datasqrl.lsp.TestLanguageServers.start;
import static com.datasqrl.lsp.TestLanguageServers.stop;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.lsp.TestLanguageServers.RecordingClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.DefinitionParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.PrepareRenameParams;
import org.eclipse.lsp4j.ReferenceContext;
import org.eclipse.lsp4j.ReferenceParams;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.TextDocumentItem;
import org.eclipse.lsp4j.services.LanguageServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Follows and documents what IMPORT statements import: scripts, tables and functions. */
class ImportsTest {

  @TempDir Path tempDir;

  private final RecordingClient client = new RecordingClient();
  private final AtomicInteger exitCode = new AtomicInteger(-1);
  private LanguageServer server;

  @BeforeEach
  void setUp() throws Exception {
    server =
        start(
            tempDir.resolve("work"),
            client,
            new ClientCapabilities(),
            exitCode,
            new InitializeResult[1]);
  }

  @AfterEach
  void tearDown() throws Exception {
    stop(server, exitCode);
  }

  @Test
  void givenImportOfAllTablesOfAScript_whenDefinitionAndHover_thenItIsTheScriptAndItsTable()
      throws Exception {
    var project = CheckTestProject.create(tempDir);
    var script =
        """
        IMPORT shop.sources.*;
        OrderStream := SELECT * FROM Orders;
        """;
    var uri = project.mainScript().toUri().toString();
    open(uri, script);
    TestLanguageServers.awaitHover(server, uri, 1, 1);

    assertThat(definition(uri, 0, "IMPORT shop.sources.".length()))
        .extracting(Location::getUri)
        .contains(project.sharedScript().toUri().toString());
    var hover = TestLanguageServers.hover(server, uri, 0, "IMPORT shop.sou".length());
    assertThat(hover.getContents().getRight().getValue()).startsWith("```sql\nOrders (");
  }

  @Test
  void givenTableImportedWithAlias_whenAtItsDefinition_thenItsOccurrencesAreTheImportsAndAliases()
      throws Exception {
    var project = CheckTestProject.create(tempDir);
    var script =
        """
        IMPORT shop.sources.Orders AS AllOrders;
        Totals := SELECT customer_id, SUM(amount) AS total FROM AllOrders GROUP BY customer_id;
        """;
    var uri = project.mainScript().toUri().toString();
    open(uri, script);
    TestLanguageServers.awaitHover(server, uri, 1, 1);
    var shared = project.sharedScript().toUri().toString();
    open(shared, Files.readString(project.sharedScript()));
    // In `Orders` of `CREATE TABLE Orders (`, which the alias renames for the main script
    var name = "CREATE TABLE Or".length();
    var hover = TestLanguageServers.awaitHover(server, shared, 0, name);

    assertThat(references(shared, 0, name))
        .extracting(l -> position(l, uri, shared))
        .containsExactlyInAnyOrder(
            "shared:0:13",
            "main:0:" + "IMPORT ".length(),
            "main:0:" + script.indexOf("AllOrders"),
            "main:1:" + script.lines().toList().get(1).indexOf("AllOrders"));
    // Editors show the occurrences of the definition itself
    assertThat(definition(shared, 0, name))
        .extracting(l -> position(l, uri, shared))
        .containsExactly("shared:0:13");
    assertThat(hover.getContents().getRight().getValue()).startsWith("```sql\nAllOrders (");
    // A rename would not rename the alias
    assertThatThrownBy(
            () ->
                server
                    .getTextDocumentService()
                    .prepareRename(
                        new PrepareRenameParams(
                            new TextDocumentIdentifier(shared), new Position(0, name)))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
        .hasMessageContaining("can be renamed");
  }

  private static String position(Location location, String main, String shared) {
    var file =
        location.getUri().equals(main)
            ? "main"
            : location.getUri().equals(shared) ? "shared" : location.getUri();
    var start = location.getRange().getStart();
    return file + ":" + start.getLine() + ":" + start.getCharacter();
  }

  private List<? extends Location> references(String uri, int line, int character)
      throws Exception {
    var params =
        new ReferenceParams(
            new TextDocumentIdentifier(uri),
            new Position(line, character),
            new ReferenceContext(true));
    return server
        .getTextDocumentService()
        .references(params)
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  void givenImportedFunction_whenDefinitionAndHover_thenItIsItsJavaSource() throws Exception {
    // The use case imports functions from a jar, whose project has their sources
    var usecase =
        Path.of("../sqrl-testing/sqrl-testing-integration/src/test/resources/usecases/udf");
    var udf = tempDir.resolve("udf");
    try (var files = Files.walk(usecase)) {
      for (var file : files.toList()) {
        var copy = udf.resolve(usecase.relativize(file).toString());
        if (Files.isDirectory(file)) {
          Files.createDirectories(copy);
        } else {
          Files.copy(file, copy);
        }
      }
    }
    var script = udf.resolve("myudf.sqrl");
    var text = Files.readString(script);
    var uri = script.toUri().toString();
    open(uri, text);
    TestLanguageServers.awaitHover(server, uri, 4, 1);

    var source = udf.resolve("myjavafunction/src/main/java/com/myudf/MyScalarFunction.java");
    var sourceLines = Files.readAllLines(source);
    var declaration =
        sourceLines.indexOf(
            sourceLines.stream()
                .filter(line -> line.contains("class MyScalarFunction"))
                .findFirst()
                .orElseThrow());
    var lines = text.lines().toList();
    var call = lines.get(4).indexOf("MyScalarFunction(");
    var aliasCall = lines.get(8).indexOf("AnotherFunction(");
    // Imported by its name and with an alias, and called by both
    for (var position :
        List.of(
            new Position(0, "IMPORT myjavafunction.lib.My".length()),
            new Position(1, "IMPORT myjavafunction.lib.My".length()),
            new Position(4, call + 2),
            new Position(8, aliasCall + 2))) {
      assertThat(definition(uri, position.getLine(), position.getCharacter()))
          .as("definition at %s", position)
          .singleElement()
          .satisfies(
              location -> {
                assertThat(location.getUri()).isEqualTo(source.toUri().toString());
                assertThat(location.getRange().getStart().getLine()).isEqualTo(declaration);
              });
    }
    var hover = TestLanguageServers.hover(server, uri, 0, "IMPORT myjavafunction.lib.My".length());
    assertThat(hover.getContents().getRight().getValue())
        .startsWith("```sql\n")
        .endsWith("User-defined function `com.myudf.MyScalarFunction`");
  }

  private List<? extends Location> definition(String uri, int line, int character)
      throws Exception {
    return server
        .getTextDocumentService()
        .definition(
            new DefinitionParams(new TextDocumentIdentifier(uri), new Position(line, character)))
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .getLeft();
  }

  private void open(String uri, String text) {
    server
        .getTextDocumentService()
        .didOpen(new DidOpenTextDocumentParams(new TextDocumentItem(uri, "sqrl", 1, text)));
  }
}
