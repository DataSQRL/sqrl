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

import static com.datasqrl.compile.check.CheckTestProject.API_SCRIPT;
import static com.datasqrl.compile.check.CheckTestProject.MAIN_SCRIPT;
import static com.datasqrl.compile.check.CheckTestProject.SHARED_SCRIPT;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.lsp4j.AnnotatedTextEdit;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.CodeActionCapabilities;
import org.eclipse.lsp4j.CodeActionContext;
import org.eclipse.lsp4j.CodeActionKind;
import org.eclipse.lsp4j.CodeActionKindCapabilities;
import org.eclipse.lsp4j.CodeActionLiteralSupportCapabilities;
import org.eclipse.lsp4j.CodeActionParams;
import org.eclipse.lsp4j.CompletionContext;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionParams;
import org.eclipse.lsp4j.CompletionTriggerKind;
import org.eclipse.lsp4j.DefinitionParams;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.DocumentHighlight;
import org.eclipse.lsp4j.DocumentHighlightKind;
import org.eclipse.lsp4j.DocumentHighlightParams;
import org.eclipse.lsp4j.DocumentSymbol;
import org.eclipse.lsp4j.DocumentSymbolParams;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.MessageActionItem;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.PrepareRenameParams;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.ReferenceContext;
import org.eclipse.lsp4j.ReferenceParams;
import org.eclipse.lsp4j.RenameCapabilities;
import org.eclipse.lsp4j.RenameParams;
import org.eclipse.lsp4j.TextDocumentClientCapabilities;
import org.eclipse.lsp4j.TextDocumentContentChangeEvent;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.TextDocumentItem;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.VersionedTextDocumentIdentifier;
import org.eclipse.lsp4j.WorkspaceClientCapabilities;
import org.eclipse.lsp4j.WorkspaceEdit;
import org.eclipse.lsp4j.WorkspaceEditCapabilities;
import org.eclipse.lsp4j.WorkspaceEditChangeAnnotationSupportCapabilities;
import org.eclipse.lsp4j.jsonrpc.ResponseErrorException;
import org.eclipse.lsp4j.services.LanguageServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs the language server and a client in-process, connected over the LSP protocol. */
class SqrlLanguageServerTest {

  @TempDir Path tempDir;

  private CheckTestProject project;
  private final RecordingClient client = new RecordingClient();
  private final AtomicInteger exitCode = new AtomicInteger(-1);
  private LanguageServer server;

  @BeforeEach
  void setUp() throws Exception {
    project = CheckTestProject.create(tempDir);
    var result = new InitializeResult[1];
    server = start(tempDir.resolve("work"), client, new ClientCapabilities(), exitCode, result);
    assertThat(result[0].getServerInfo().getName()).isEqualTo("sqrl");
    assertThat(result[0].getCapabilities().getHoverProvider().getLeft()).isTrue();
    assertThat(result[0].getCapabilities().getCompletionProvider().getTriggerCharacters())
        .contains(".");
    assertThat(result[0].getCapabilities().getRenameProvider().getLeft()).isTrue();
    // Without literal support, an editor would expect commands
    assertThat(result[0].getCapabilities().getCodeActionProvider()).isNull();
  }

  @AfterEach
  void tearDown() throws Exception {
    stop(server, exitCode);
  }

  @Test
  void givenScriptWithError_whenOpenedAndFixed_thenPublishesAndClearsDiagnostics()
      throws Exception {
    var uri = project.mainScript().toUri().toString();
    var broken = MAIN_SCRIPT.replace("SUM(amount) AS total", "SUM(amounts) AS total");
    open(uri, broken);

    var published = client.await(uri, diagnostics -> !diagnostics.isEmpty());
    assertThat(published)
        .singleElement()
        .satisfies(
            diagnostic -> {
              assertThat(diagnostic.getMessage().getLeft()).contains("Column 'amounts' not found");
              assertThat(diagnostic.getRange())
                  .isEqualTo(new Range(new Position(4, 42), new Position(4, 49)));
            });

    change(uri, 2, MAIN_SCRIPT);

    assertThat(client.await(uri, List::isEmpty)).isEmpty();
  }

  @Test
  void givenUnknownColumn_whenCodeAction_thenSuggestsTheClosestColumn() throws Exception {
    var uri = project.mainScript().toUri().toString();
    open(uri, MAIN_SCRIPT.replace("SUM(amount) AS total", "SUM(amout) AS total"));
    var diagnostics = client.await(uri, published -> !published.isEmpty());
    var params =
        new CodeActionParams(
            new TextDocumentIdentifier(uri),
            diagnostics.get(0).getRange(),
            new CodeActionContext(diagnostics));

    var actions =
        server.getTextDocumentService().codeAction(params).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(actions)
        .singleElement()
        .satisfies(
            action -> {
              assertThat(action.getRight().getTitle()).isEqualTo("Change to `amount`");
              assertThat(action.getRight().getEdit().getChanges().get(uri))
                  .containsExactly(
                      new TextEdit(new Range(new Position(4, 42), new Position(4, 47)), "amount"));
            });
    params.getContext().setOnly(List.of(CodeActionKind.Refactor));
    assertThat(
            server
                .getTextDocumentService()
                .codeAction(params)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
        .isEmpty();
  }

  @Test
  void givenScriptWithoutProject_whenOpened_thenWarnsThatItIsNotChecked() throws Exception {
    var script = Files.writeString(tempDir.resolve("loose.sqrl"), "X := SELECT 1 AS one;\n");
    var uri = script.toUri().toString();
    open(uri, Files.readString(script));

    assertThat(client.await(uri, published -> !published.isEmpty()))
        .singleElement()
        .satisfies(
            d -> assertThat(d.getMessage().getLeft()).startsWith("This script is not checked"));
    assertThatThrownBy(() -> rename(uri, 0, 0, "Y"))
        .cause()
        .hasMessage("`loose.sqrl` is not part of a project, see its warning.");
  }

  @Test
  void givenErrorInImportedScript_whenOpened_thenPublishesItForThatScript() throws Exception {
    var mainUri = project.mainScript().toUri().toString();
    open(mainUri, MAIN_SCRIPT.replace("SUM(amount) AS total", "SUM(amounts) AS total"));
    client.await(mainUri, diagnostics -> !diagnostics.isEmpty());

    var sharedUri = project.sharedScript().toUri().toString();
    open(sharedUri, SHARED_SCRIPT.replace("PRIMARY KEY (id,", "PRIMARY KEY (idx,"));

    var published = client.await(sharedUri, diagnostics -> !diagnostics.isEmpty());
    assertThat(published)
        .singleElement()
        .satisfies(
            diagnostic ->
                assertThat(diagnostic.getRange())
                    .isEqualTo(new Range(new Position(1, 15), new Position(1, 18))));
  }

  @Test
  void givenCheckedScript_whenHover_thenDescribesTablesAndColumns() throws Exception {
    var uri = project.mainScript().toUri().toString();
    open(uri, MAIN_SCRIPT);

    // Orders in `FROM Orders` of CustomerTotals
    var table = markdown(awaitHover(uri, 4, 66));
    assertThat(table)
        .contains("Orders (", "  customer_id BIGINT", "Versioned state table, primary key `id`")
        .contains("`main.sqrl` line 4");
    // amount in `SUM(amount)`
    assertThat(markdown(hover(uri, 4, 44))).contains("```sql\namount ", "Column of `Orders`");
    // OrderStream in `DISTINCT OrderStream`, which is looked up by name
    assertThat(markdown(hover(uri, 3, 22))).contains("OrderStream (", "`main.sqrl` line 3");
    assertThat(hover(uri, 1, 0)).isNull();
  }

  @Test
  void givenCheckedScript_whenDefinition_thenFindsImportedTablesAndScripts() throws Exception {
    var uri = project.mainScript().toUri().toString();
    open(uri, MAIN_SCRIPT);
    awaitHover(uri, 4, 66);
    var sharedUri = project.sharedScript().toUri().toString();

    // Orders in `sources.Orders`
    assertThat(definition(uri, 2, 40))
        .containsExactly(
            new Location(sharedUri, new Range(new Position(0, 13), new Position(0, 19))));
    // the path of the IMPORT statement
    assertThat(definition(uri, 0, 10))
        .containsExactly(
            new Location(sharedUri, new Range(new Position(0, 0), new Position(0, 0))));
    // customer_id in `GROUP BY customer_id` goes to its table
    assertThat(definition(uri, 4, 85))
        .containsExactly(new Location(uri, new Range(new Position(3, 0), new Position(3, 6))));
  }

  @Test
  void givenCheckedScript_whenDocumentSymbol_thenOutlinesItWithColumns() throws Exception {
    var uri = project.mainScript().toUri().toString();
    open(uri, MAIN_SCRIPT);
    awaitHover(uri, 4, 66);

    var outline =
        server
            .getTextDocumentService()
            .documentSymbol(new DocumentSymbolParams(new TextDocumentIdentifier(uri)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .stream()
            .map(symbol -> symbol.getRight())
            .toList();

    assertThat(outline)
        .extracting(DocumentSymbol::getName)
        .containsExactly("IMPORT shop.sources", "OrderStream", "Orders", "CustomerTotals");
    assertThat(outline.get(3).getChildren())
        .extracting(DocumentSymbol::getName)
        .containsExactly("customer_id", "total");
  }

  @Test
  void givenUncheckedEdit_whenCompletion_thenCompletesWithTheLastCatalog() throws Exception {
    var uri = project.mainScript().toUri().toString();
    open(uri, MAIN_SCRIPT);
    awaitHover(uri, 4, 66);

    // Typing a new statement, which is completed before it is checked
    var typed = "Big := SELECT o. FROM Orders o";
    change(uri, 2, MAIN_SCRIPT + typed);
    var items =
        server
            .getTextDocumentService()
            .completion(
                new CompletionParams(
                    new TextDocumentIdentifier(uri), new Position(5, typed.indexOf('.') + 1)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .getRight()
            .getItems();

    assertThat(items)
        .extracting(CompletionItem::getLabel)
        .contains("id", "customer_id", "amount", "updated_at");
    // A parenthesis completes the arguments of hints, but nothing in queries
    var call = new CompletionParams(new TextDocumentIdentifier(uri), new Position(5, 7));
    call.setContext(new CompletionContext(CompletionTriggerKind.TriggerCharacter, "("));
    var completion = server.getTextDocumentService().completion(call);
    assertThat(completion.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getRight().getItems()).isEmpty();
  }

  @Test
  void givenCheckedScript_whenReferencesAndHighlight_thenFindsTheNamesOfTheTable()
      throws Exception {
    var uri = project.mainScript().toUri().toString();
    open(uri, MAIN_SCRIPT);
    awaitHover(uri, 4, 66);

    // Orders in `FROM Orders` of CustomerTotals
    var references =
        server
            .getTextDocumentService()
            .references(
                new ReferenceParams(
                    new TextDocumentIdentifier(uri),
                    new Position(4, 66),
                    new ReferenceContext(true)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    var highlights =
        server
            .getTextDocumentService()
            .documentHighlight(
                new DocumentHighlightParams(new TextDocumentIdentifier(uri), new Position(4, 66)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(List.<Location>copyOf(references))
        .containsExactlyInAnyOrder(
            new Location(uri, new Range(new Position(3, 0), new Position(3, 6))),
            new Location(uri, new Range(new Position(4, 64), new Position(4, 70))));
    assertThat(List.<DocumentHighlight>copyOf(highlights))
        .containsExactlyInAnyOrder(
            new DocumentHighlight(
                new Range(new Position(3, 0), new Position(3, 6)), DocumentHighlightKind.Write),
            new DocumentHighlight(
                new Range(new Position(4, 64), new Position(4, 70)), DocumentHighlightKind.Read));
  }

  @Test
  void givenTableOfApi_whenRename_thenAsksToConfirmAndRenamesItInScriptAndApi() throws Exception {
    var apiProject = CheckTestProject.createWithApi(tempDir.resolve("api"));
    var uri = apiProject.mainScript().toUri().toString();
    open(uri, API_SCRIPT);
    awaitHover(uri, 4, 66);
    client.answer(RENAME);

    var prepared =
        server
            .getTextDocumentService()
            .prepareRename(
                new PrepareRenameParams(new TextDocumentIdentifier(uri), new Position(3, 2)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .getSecond();
    var edit = rename(uri, 3, 2, "Purchases");

    assertThat(prepared.getRange()).isEqualTo(new Range(new Position(3, 0), new Position(3, 6)));
    assertThat(prepared.getPlaceholder()).isEqualTo("Orders");
    assertThat(edit.getChanges())
        .containsOnlyKeys(
            uri,
            apiProject.schema().toUri().toString(),
            apiProject.operations().toUri().toString());
    assertThat(edit.getChanges().get(uri))
        .extracting(TextEdit::getNewText)
        .containsOnly("Purchases")
        .hasSize(3);
    assertThat(edit.getChanges().get(apiProject.schema().toUri().toString())).hasSize(3);
    assertThat(client.messageRequests)
        .singleElement()
        .satisfies(
            request ->
                assertThat(request.getMessage())
                    .contains("GraphQL API", "`schema.graphqls`", "`queries.graphql`"));
  }

  @Test
  void givenRenameThatBreaksTheApi_whenRename_thenRefusesWithTheError() throws Exception {
    var apiProject = CheckTestProject.createWithApi(tempDir.resolve("api"));
    var uri = apiProject.mainScript().toUri().toString();
    open(uri, API_SCRIPT);
    awaitHover(uri, 4, 66);
    client.answer(RENAME);

    // A table whose name starts with _ is hidden, so the API cannot query it
    assertThatThrownBy(() -> rename(uri, 3, 2, "_Orders"))
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOf(ResponseErrorException.class)
        .hasMessageContaining("Renaming `Orders` to `_Orders` would cause an error");
    assertThatThrownBy(() -> rename(uri, 4, 85, "customer"))
        .cause()
        .hasMessage("Columns cannot be renamed yet.");
  }

  @Test
  void givenEditorWithChangeAnnotations_whenRename_thenAnnotatesTheEditsOfTheApi()
      throws Exception {
    var capabilities = new ClientCapabilities();
    var workspaceEdit = new WorkspaceEditCapabilities();
    workspaceEdit.setDocumentChanges(true);
    workspaceEdit.setChangeAnnotationSupport(
        new WorkspaceEditChangeAnnotationSupportCapabilities(true));
    var workspace = new WorkspaceClientCapabilities();
    workspace.setWorkspaceEdit(workspaceEdit);
    capabilities.setWorkspace(workspace);
    var textDocument = new TextDocumentClientCapabilities();
    textDocument.setRename(new RenameCapabilities(true, false));
    textDocument.setCodeAction(
        new CodeActionCapabilities(
            new CodeActionLiteralSupportCapabilities(
                new CodeActionKindCapabilities(List.of(CodeActionKind.QuickFix))),
            false));
    capabilities.setTextDocument(textDocument);
    var annotatingClient = new RecordingClient();
    var annotatingExit = new AtomicInteger(-1);
    var result = new InitializeResult[1];
    var annotatingServer =
        start(tempDir.resolve("work2"), annotatingClient, capabilities, annotatingExit, result);
    try {
      var apiProject = CheckTestProject.createWithApi(tempDir.resolve("api"));
      var uri = apiProject.mainScript().toUri().toString();
      annotatingServer
          .getTextDocumentService()
          .didOpen(new DidOpenTextDocumentParams(new TextDocumentItem(uri, "sqrl", 1, API_SCRIPT)));
      TestLanguageServers.awaitHover(annotatingServer, uri, 4, 66);

      var edit =
          annotatingServer
              .getTextDocumentService()
              .rename(
                  new RenameParams(
                      new TextDocumentIdentifier(uri), new Position(5, 9), "customerTotals"))
              .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      assertThat(result[0].getCapabilities().getRenameProvider().getRight().getPrepareProvider())
          .isTrue();
      assertThat(
              result[0].getCapabilities().getCodeActionProvider().getRight().getCodeActionKinds())
          .containsExactly(CodeActionKind.QuickFix);
      assertThat(edit.getChangeAnnotations().get(RenameRequests.API_CHANGE))
          .satisfies(annotation -> assertThat(annotation.getNeedsConfirmation()).isTrue());
      assertThat(edit.getDocumentChanges())
          .extracting(change -> change.getLeft().getTextDocument().getUri())
          .containsExactlyInAnyOrder(
              uri,
              apiProject.schema().toUri().toString(),
              apiProject.operations().toUri().toString());
      assertThat(edit.getDocumentChanges())
          .flatExtracting(change -> change.getLeft().getEdits())
          .extracting(e -> e.getLeft() instanceof AnnotatedTextEdit)
          .containsExactlyInAnyOrder(false, true, true);
      assertThat(annotatingClient.messageRequests).isEmpty();
    } finally {
      stop(annotatingServer, annotatingExit);
    }
  }

  private WorkspaceEdit rename(String uri, int line, int character, String newName)
      throws Exception {
    return server
        .getTextDocumentService()
        .rename(
            new RenameParams(
                new TextDocumentIdentifier(uri), new Position(line, character), newName))
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private Hover hover(String uri, int line, int character) throws Exception {
    return TestLanguageServers.hover(server, uri, line, character);
  }

  /** Waits until the project has been checked, so that the hover is known. */
  private Hover awaitHover(String uri, int line, int character) throws Exception {
    return TestLanguageServers.awaitHover(server, uri, line, character);
  }

  private static String markdown(Hover hover) {
    assertThat(hover).isNotNull();
    return hover.getContents().getRight().getValue();
  }

  private List<Location> definition(String uri, int line, int character) throws Exception {
    return List.copyOf(
        server
            .getTextDocumentService()
            .definition(
                new DefinitionParams(
                    new TextDocumentIdentifier(uri), new Position(line, character)))
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .getLeft());
  }

  private void open(String uri, String text) {
    server
        .getTextDocumentService()
        .didOpen(new DidOpenTextDocumentParams(new TextDocumentItem(uri, "sqrl", 1, text)));
  }

  private void change(String uri, int version, String text) {
    server
        .getTextDocumentService()
        .didChange(
            new DidChangeTextDocumentParams(
                new VersionedTextDocumentIdentifier(uri, version),
                List.of(new TextDocumentContentChangeEvent(text))));
  }

  private static final MessageActionItem RENAME =
      new MessageActionItem(RenameRequests.RENAME_ACTION);
}
