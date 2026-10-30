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

import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.lsp.TestLanguageServers.RecordingClient;
import com.google.gson.JsonPrimitive;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.CodeLensParams;
import org.eclipse.lsp4j.CodeLensWorkspaceCapabilities;
import org.eclipse.lsp4j.CompletionParams;
import org.eclipse.lsp4j.DefinitionParams;
import org.eclipse.lsp4j.DiagnosticSeverity;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.InlayHint;
import org.eclipse.lsp4j.InlayHintParams;
import org.eclipse.lsp4j.InlayHintWorkspaceCapabilities;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.ParameterInformationCapabilities;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.SemanticTokensParams;
import org.eclipse.lsp4j.SemanticTokensWorkspaceCapabilities;
import org.eclipse.lsp4j.SignatureHelp;
import org.eclipse.lsp4j.SignatureHelpCapabilities;
import org.eclipse.lsp4j.SignatureHelpParams;
import org.eclipse.lsp4j.SignatureInformationCapabilities;
import org.eclipse.lsp4j.SymbolKind;
import org.eclipse.lsp4j.TextDocumentClientCapabilities;
import org.eclipse.lsp4j.TextDocumentContentChangeEvent;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.TextDocumentItem;
import org.eclipse.lsp4j.VersionedTextDocumentIdentifier;
import org.eclipse.lsp4j.WorkspaceClientCapabilities;
import org.eclipse.lsp4j.WorkspaceSymbol;
import org.eclipse.lsp4j.WorkspaceSymbolParams;
import org.eclipse.lsp4j.services.LanguageServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the language server for the features that come from checks besides diagnostics: semantic
 * tokens, inlay hints, signature help, workspace symbols, and those of package files and templates.
 */
class LanguageFeaturesTest {

  private static final String SCRIPT =
      """
      IMPORT shop.sources AS src;

      OrderStream := SELECT * FROM src.Orders;
      OrderStream.doubled := amount * 2;
      Orders := DISTINCT OrderStream ON id ORDER BY updated_at DESC;
      CustomerTotals := SELECT customer_id, SUM(amount) AS total FROM Orders GROUP BY customer_id;
      OrdersOf(cid BIGINT NOT NULL) := SELECT * FROM Orders WHERE customer_id = :cid;
      OrderNames := SELECT id, SUBSTRING(CAST(id AS STRING), 1, 2) AS prefix FROM Orders;
      """;

  @TempDir Path tempDir;

  private CheckTestProject project;
  private String uri;
  private final RecordingClient client = new RecordingClient();
  private final AtomicInteger exitCode = new AtomicInteger(-1);
  private LanguageServer server;
  private InitializeResult initialized;

  @BeforeEach
  void setUp() throws Exception {
    project = CheckTestProject.create(tempDir);
    uri = project.mainScript().toUri().toString();
    var workspace = new WorkspaceClientCapabilities();
    workspace.setSemanticTokens(new SemanticTokensWorkspaceCapabilities(true));
    workspace.setInlayHint(new InlayHintWorkspaceCapabilities(true));
    workspace.setCodeLens(new CodeLensWorkspaceCapabilities(true));
    var information = new SignatureInformationCapabilities();
    information.setParameterInformation(new ParameterInformationCapabilities(true));
    var signatureHelp = new SignatureHelpCapabilities();
    signatureHelp.setSignatureInformation(information);
    var textDocument = new TextDocumentClientCapabilities();
    textDocument.setSignatureHelp(signatureHelp);
    var capabilities = new ClientCapabilities(workspace, textDocument, null);
    var result = new InitializeResult[1];
    server = start(tempDir.resolve("work"), client, capabilities, exitCode, result);
    initialized = result[0];
  }

  @AfterEach
  void tearDown() throws Exception {
    stop(server, exitCode);
  }

  @Test
  void givenCheckedScript_whenSemanticTokens_thenColorsNamesByWhatTheyAre() throws Exception {
    var legend = initialized.getCapabilities().getSemanticTokensProvider().getLegend();
    assertThat(legend.getTokenTypes())
        .containsExactly("class", "function", "method", "property", "namespace");
    open(uri, SCRIPT);
    awaitCheck();

    var tokens = semanticTokens();
    // The database and the table of `src.Orders`, and the alias that names the database
    assertThat(tokens)
        .contains(
            new Token(0, "IMPORT shop.sources AS ".length(), 3, "namespace", true),
            new Token(2, 0, "OrderStream".length(), "class", true),
            new Token(2, "OrderStream := SELECT * FROM ".length(), 3, "namespace", false),
            new Token(2, SCRIPT.lines().toList().get(2).indexOf("Orders;"), 6, "class", false),
            new Token(3, "OrderStream.".length(), "doubled".length(), "property", true),
            new Token(6, 0, "OrdersOf".length(), "function", true));
    var amount = SCRIPT.lines().toList().get(5).indexOf("amount");
    assertThat(tokens).contains(new Token(5, amount, "amount".length(), "property", false));
    assertThat(client.semanticTokensRefreshes.get()).isPositive();
    assertThat(client.inlayHintsRefreshes.get()).isPositive();
  }

  @Test
  void givenComputedColumns_whenInlayHints_thenShowsTheirTypes() throws Exception {
    open(uri, SCRIPT);
    awaitCheck();

    var hints = inlayHints();

    var lines = SCRIPT.lines().toList();
    assertThat(hints)
        .extracting(InlayHint::getPosition)
        .containsExactly(
            new Position(3, "OrderStream.doubled".length()),
            new Position(5, lines.get(5).indexOf(" FROM")),
            new Position(7, lines.get(7).indexOf(" FROM")));
    assertThat(hints)
        .extracting(hint -> hint.getLabel().getLeft())
        .satisfiesExactly(
            label -> assertThat(label).startsWith(": DOUBLE"),
            label -> assertThat(label).startsWith(": DOUBLE"),
            label -> assertThat(label).isEqualTo(": STRING"));

    // Right after an edit, the hints wait for the check of the edit
    var edited = SCRIPT.replace("AS prefix FROM", "AS prefix, id * 2 AS twice FROM");
    change(uri, 2, edited);
    var twice = edited.lines().toList().get(7).indexOf(" FROM");
    assertThat(inlayHints())
        .filteredOn(hint -> hint.getPosition().equals(new Position(7, twice)))
        .extracting(hint -> hint.getLabel().getLeft())
        .containsExactly(": BIGINT NOT NULL");
  }

  @Test
  void givenScriptThatFailsToCheck_whenInlayHintsAfterTheCheck_thenChecksNoMore() throws Exception {
    open(uri, "Broken := SELECT * FROM Missing;\n");
    client.await(uri, diagnostics -> !diagnostics.isEmpty());
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (client.semanticTokensRefreshes.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    var refreshes = client.semanticTokensRefreshes.get();

    // The script has no symbols, but the editor asks for hints after each refresh, so another
    // check would refresh again, and so on
    assertThat(inlayHints()).isEmpty();
    assertThat(client.semanticTokensRefreshes.get()).isEqualTo(refreshes);
  }

  private List<InlayHint> inlayHints() throws Exception {
    return server
        .getTextDocumentService()
        .inlayHint(
            new InlayHintParams(
                new TextDocumentIdentifier(uri),
                new Range(new Position(0, 0), new Position(10, 0))))
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  void givenFunctionCall_whenSignatureHelpAndHover_thenShowsTheSignatures() throws Exception {
    open(uri, SCRIPT);
    awaitCheck();
    var lines = SCRIPT.lines().toList();

    // A built-in function, at its third argument
    var help = signatureHelp(7, lines.get(7).indexOf("2)"));
    assertThat(help).isNotNull();
    var signature = help.getSignatures().get(help.getActiveSignature());
    assertThat(signature.getLabel()).startsWith("SUBSTRING(");
    assertThat(help.getActiveParameter()).isEqualTo(2);
    var parameter = signature.getParameters().get(2).getLabel().getRight();
    assertThat(signature.getLabel().substring(parameter.getFirst(), parameter.getSecond()))
        .isNotBlank();

    // A table function of the script, while its call is typed
    var typed = SCRIPT + "Calls := SELECT * FROM TABLE(OrdersOf(";
    change(uri, 2, typed);
    help = signatureHelp(8, "Calls := SELECT * FROM TABLE(OrdersOf(".length());
    assertThat(help.getSignatures())
        .singleElement()
        .satisfies(s -> assertThat(s.getLabel()).isEqualTo("OrdersOf(cid BIGINT NOT NULL)"));
    assertThat(help.getActiveParameter()).isZero();

    // Not in a string
    change(uri, 3, SCRIPT + "Calls := SELECT 'SUBSTRING(");
    assertThat(signatureHelp(8, "Calls := SELECT 'SUBSTRING(".length())).isNull();

    // A built-in function that is hovered
    change(uri, 4, SCRIPT);
    var hover = TestLanguageServers.hover(server, uri, 5, lines.get(5).indexOf("SUM") + 1);
    assertThat(hover.getContents().getRight().getValue()).startsWith("```sql\nSUM(");
  }

  @Test
  void givenCheckedProjects_whenWorkspaceSymbols_thenFindsDefinitionsByAbbreviation()
      throws Exception {
    open(uri, SCRIPT);
    awaitCheck();

    var symbols =
        server
            .getWorkspaceService()
            .symbol(new WorkspaceSymbolParams("custot"))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .getRight();

    assertThat(symbols)
        .singleElement()
        .satisfies(
            symbol -> {
              assertThat(symbol.getName()).isEqualTo("CustomerTotals");
              assertThat(symbol.getKind()).isEqualTo(SymbolKind.Class);
              assertThat(symbol.getLocation().getLeft().getUri()).isEqualTo(uri);
            });
    var all =
        server
            .getWorkspaceService()
            .symbol(new WorkspaceSymbolParams(""))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .getRight();
    assertThat(all)
        .extracting(WorkspaceSymbol::getName)
        .contains("OrderStream", "Orders", "OrdersOf", "CustomerTotals");
  }

  @Test
  void givenImportAlias_whenDefinition_thenGoesToTheImportedScript() throws Exception {
    open(uri, SCRIPT);
    awaitCheck();

    var locations =
        List.copyOf(
            server
                .getTextDocumentService()
                .definition(
                    new DefinitionParams(
                        new TextDocumentIdentifier(uri),
                        new Position(0, "IMPORT shop.sources AS s".length())))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getLeft());

    assertThat(locations)
        .extracting(Location::getUri)
        .containsExactly(project.sharedScript().toUri().toString());
  }

  @Test
  void givenTemplateVariableOfAColumn_whenColoredAndHovered_thenItIsTheColumn() throws Exception {
    var packageFile = project.mainScript().resolveSibling("package.json");
    Files.writeString(
        packageFile,
        Files.readString(packageFile)
            .replace(
                "\"main\": \"main.sqrl\",",
                "\"main\": \"main.sqrl\", \"config\": {\"column\": \"amount\"},"));
    var script =
        """
        IMPORT shop.sources AS src;
        OrderStream := SELECT * FROM src.Orders;
        Tested := SELECT {{column}} FROM OrderStream;
        """;
    open(uri, script);
    awaitCheck();

    var variable = script.lines().toList().get(2).indexOf("column");
    assertThat(semanticTokens())
        .contains(new Token(2, variable, "column".length(), "property", false));
    assertThat(
            TestLanguageServers.hover(server, uri, 2, variable).getContents().getRight().getValue())
        .startsWith("`column` = `amount`")
        .contains("DOUBLE");
  }

  @Test
  void givenTestTable_whenCodeLens_thenItRunsTheTestsOfTheProject() throws Exception {
    var script = SCRIPT + "/*+test */\nTotalsTest := SELECT * FROM CustomerTotals;\n";
    open(uri, script);
    awaitCheck();

    var lenses =
        server
            .getTextDocumentService()
            .codeLens(new CodeLensParams(new TextDocumentIdentifier(uri)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    var line = script.lines().toList().indexOf("TotalsTest := SELECT * FROM CustomerTotals;");
    var packageFile = project.mainScript().resolveSibling("package.json").toUri().toString();
    assertThat(lenses)
        .singleElement()
        .satisfies(
            lens -> {
              assertThat(lens.getRange().getStart().getLine()).isEqualTo(line);
              assertThat(lens.getCommand().getCommand()).isEqualTo(TestLenses.RUN_TESTS_COMMAND);
              assertThat(lens.getCommand().getArguments())
                  .extracting(argument -> ((JsonPrimitive) argument).getAsString())
                  .containsExactly(packageFile);
            });
    assertThat(client.codeLensRefreshes.get()).isPositive();
  }

  @Test
  void givenUnknownTemplateVariable_whenChecked_thenWarns() throws Exception {
    open(uri, SCRIPT);
    awaitCheck();
    var shared = project.sharedScript().toUri().toString();
    var typo = CheckTestProject.SHARED_SCRIPT.replace("{{interval}}", "{{intervall}}");
    open(shared, typo);

    var diagnostics =
        client.await(
            shared,
            published ->
                published.stream()
                    .anyMatch(
                        d -> d.getMessage().getLeft().contains("`intervall` is not defined")));

    assertThat(diagnostics)
        .filteredOn(d -> d.getMessage().getLeft().contains("intervall"))
        .singleElement()
        .satisfies(
            d -> {
              assertThat(d.getSeverity()).isEqualTo(DiagnosticSeverity.Warning);
              var line = typo.lines().toList().get(4);
              assertThat(d.getRange())
                  .isEqualTo(
                      new Range(
                          new Position(4, line.indexOf("{{")),
                          new Position(4, line.indexOf("}}") + 2)));
            });
  }

  @Test
  void givenPackageFileWithError_whenOpened_thenItHasTheDiagnostics() throws Exception {
    var packageFile = project.projectRoot().resolve("package.json");
    var config = Files.readString(packageFile).replace("\"flink\"]", "\"flink\", \"nope\"]");
    Files.writeString(packageFile, config);
    var packageUri = packageFile.toUri().toString();

    open(packageUri, config);

    var diagnostics = client.await(packageUri, published -> !published.isEmpty());
    // At the value that the message is about
    var line =
        config
            .lines()
            .toList()
            .indexOf(config.lines().filter(l -> l.contains("nope")).findFirst().orElseThrow());
    assertThat(diagnostics)
        .filteredOn(d -> d.getMessage().getLeft().contains("/enabled-engines/4"))
        .isNotEmpty()
        .allSatisfy(
            d ->
                assertThat(d.getRange())
                    .isEqualTo(
                        new Range(
                            new Position(
                                line, config.lines().toList().get(line).indexOf("\"nope\"")),
                            new Position(
                                line, config.lines().toList().get(line).indexOf("\"nope\"") + 6))));
    // Requests about scripts are not answered for package files
    var completion =
        server
            .getTextDocumentService()
            .completion(
                new CompletionParams(new TextDocumentIdentifier(packageUri), new Position(1, 2)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(completion.getRight().getItems()).isEmpty();
  }

  /** A decoded semantic token. */
  private record Token(int line, int start, int length, String type, boolean declaration) {}

  private List<Token> semanticTokens() throws Exception {
    var legend = initialized.getCapabilities().getSemanticTokensProvider().getLegend();
    var data =
        server
            .getTextDocumentService()
            .semanticTokensFull(new SemanticTokensParams(new TextDocumentIdentifier(uri)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .getData();
    var tokens = new ArrayList<Token>();
    var line = 0;
    var start = 0;
    for (var i = 0; i < data.size(); i += 5) {
      start = data.get(i) == 0 ? start + data.get(i + 1) : data.get(i + 1);
      line += data.get(i);
      tokens.add(
          new Token(
              line,
              start,
              data.get(i + 2),
              legend.getTokenTypes().get(data.get(i + 3)),
              (data.get(i + 4) & 1) != 0));
    }
    return tokens;
  }

  private SignatureHelp signatureHelp(int line, int character) throws Exception {
    return server
        .getTextDocumentService()
        .signatureHelp(
            new SignatureHelpParams(new TextDocumentIdentifier(uri), new Position(line, character)))
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /** Waits until the script has been checked. */
  private Hover awaitCheck() throws Exception {
    var hover = TestLanguageServers.awaitHover(server, uri, 2, 1);
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (client.semanticTokensRefreshes.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    return hover;
  }

  private void open(String documentUri, String text) {
    server
        .getTextDocumentService()
        .didOpen(new DidOpenTextDocumentParams(new TextDocumentItem(documentUri, "sqrl", 1, text)));
  }

  private void change(String documentUri, int version, String text) {
    server
        .getTextDocumentService()
        .didChange(
            new DidChangeTextDocumentParams(
                new VersionedTextDocumentIdentifier(documentUri, version),
                List.of(new TextDocumentContentChangeEvent(text))));
  }
}
