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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.IntConsumer;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.lsp4j.AnnotatedTextEdit;
import org.eclipse.lsp4j.ClientCapabilities;
import org.eclipse.lsp4j.CodeActionCapabilities;
import org.eclipse.lsp4j.CodeActionKind;
import org.eclipse.lsp4j.CodeActionOptions;
import org.eclipse.lsp4j.CodeLensOptions;
import org.eclipse.lsp4j.CodeLensWorkspaceCapabilities;
import org.eclipse.lsp4j.CompletionOptions;
import org.eclipse.lsp4j.ExecuteCommandOptions;
import org.eclipse.lsp4j.InitializeParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.InlayHintWorkspaceCapabilities;
import org.eclipse.lsp4j.ParameterInformationCapabilities;
import org.eclipse.lsp4j.RenameCapabilities;
import org.eclipse.lsp4j.RenameOptions;
import org.eclipse.lsp4j.SaveOptions;
import org.eclipse.lsp4j.SemanticTokensWithRegistrationOptions;
import org.eclipse.lsp4j.SemanticTokensWorkspaceCapabilities;
import org.eclipse.lsp4j.ServerCapabilities;
import org.eclipse.lsp4j.ServerInfo;
import org.eclipse.lsp4j.SignatureHelpCapabilities;
import org.eclipse.lsp4j.SignatureHelpOptions;
import org.eclipse.lsp4j.SignatureInformationCapabilities;
import org.eclipse.lsp4j.TextDocumentClientCapabilities;
import org.eclipse.lsp4j.TextDocumentSyncKind;
import org.eclipse.lsp4j.TextDocumentSyncOptions;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.WorkspaceClientCapabilities;
import org.eclipse.lsp4j.WorkspaceEditCapabilities;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.launch.LSPLauncher;
import org.eclipse.lsp4j.services.LanguageClient;
import org.eclipse.lsp4j.services.LanguageClientAware;
import org.eclipse.lsp4j.services.LanguageServer;
import org.eclipse.lsp4j.services.TextDocumentService;
import org.eclipse.lsp4j.services.WorkspaceService;

/**
 * The SQRL language server. It checks the projects of open scripts with the compiler and publishes
 * the compiler's errors, warnings and notices as diagnostics. From what the compiler planned, it
 * provides the outline of scripts, hovers, the definitions and references of tables and columns,
 * completion, function signatures, semantic highlighting, the types of computed columns, the
 * renaming of tables, functions and relationships, and quick fixes for common errors.
 */
@Slf4j
public class SqrlLanguageServer implements LanguageServer, LanguageClientAware {

  private final OpenDocuments documents = new OpenDocuments();
  private final RequestThread requests = new RequestThread();
  private final ProjectChecks checks;
  private final SqrlTextDocumentService textDocumentService;
  private final SqrlWorkspaceService workspaceService;
  private final IntConsumer exitHandler;
  private volatile boolean shutdownRequested = false;

  /**
   * @param workRoot a private directory for the build output of the checked projects
   * @param exitHandler called with the exit code when the editor asks the server to exit
   */
  public SqrlLanguageServer(Path workRoot, IntConsumer exitHandler) {
    this.checks = new ProjectChecks(documents, workRoot);
    this.textDocumentService = new SqrlTextDocumentService(documents, checks, requests);
    this.workspaceService = new SqrlWorkspaceService(documents, checks, requests);
    this.exitHandler = exitHandler;
  }

  /** Runs the server on the given streams until the editor closes the connection. */
  public static void run(InputStream in, OutputStream out, Path workRoot, IntConsumer exitHandler)
      throws InterruptedException, ExecutionException {
    var server = new SqrlLanguageServer(workRoot, exitHandler);
    var launcher =
        new LSPLauncher.Builder<LanguageClient>()
            .setLocalService(server)
            .setRemoteInterface(LanguageClient.class)
            .setInput(in)
            .setOutput(out)
            .configureGson(SqrlLanguageServer::configureGson)
            .create();
    server.connect(launcher.getRemoteProxy());
    launcher.startListening().get();
  }

  /**
   * Configures the JSON mapping of the protocol. LSP4J maps the edits of a document change as plain
   * text edits, which drops the annotation of an {@link AnnotatedTextEdit}, so they are mapped with
   * their annotation.
   */
  static void configureGson(GsonBuilder gson) {
    gson.registerTypeAdapterFactory(new AnnotatedTextEdits());
  }

  /** Maps text edits with an annotation as {@link AnnotatedTextEdit}. */
  private static class AnnotatedTextEdits implements TypeAdapterFactory {

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
      if (type.getRawType() != TextEdit.class) {
        return null;
      }
      var plain = gson.getDelegateAdapter(this, TypeToken.get(TextEdit.class));
      var annotated = gson.getAdapter(AnnotatedTextEdit.class);
      var json = gson.getAdapter(JsonElement.class);
      return (TypeAdapter<T>)
          new TypeAdapter<TextEdit>() {
            @Override
            public void write(JsonWriter out, TextEdit edit) throws IOException {
              if (edit instanceof AnnotatedTextEdit annotatedEdit) {
                annotated.write(out, annotatedEdit);
              } else {
                plain.write(out, edit);
              }
            }

            @Override
            public TextEdit read(JsonReader in) throws IOException {
              var element = json.read(in);
              return element.isJsonObject() && element.getAsJsonObject().has("annotationId")
                  ? annotated.fromJsonTree(element)
                  : plain.fromJsonTree(element);
            }
          };
    }
  }

  @Override
  public void connect(LanguageClient client) {
    checks.connect(client);
    textDocumentService.connect(client);
  }

  @Override
  public CompletableFuture<InitializeResult> initialize(InitializeParams params) {
    checks.setSettings(LspSettings.from(params.getInitializationOptions()));

    var sync = new TextDocumentSyncOptions();
    sync.setOpenClose(true);
    sync.setChange(TextDocumentSyncKind.Full);
    sync.setSave(new SaveOptions(false));
    var capabilities = new ServerCapabilities();
    capabilities.setTextDocumentSync(sync);
    capabilities.setDocumentSymbolProvider(true);
    capabilities.setHoverProvider(true);
    capabilities.setDefinitionProvider(true);
    capabilities.setCompletionProvider(
        new CompletionOptions(
            false,
            Stream.of(
                    Stream.of(".", SqrlTextDocumentService.TEMPLATE_TRIGGER),
                    SqrlTextDocumentService.HINT_TRIGGERS.stream())
                .flatMap(triggers -> triggers)
                .toList()));
    capabilities.setReferencesProvider(true);
    capabilities.setWorkspaceSymbolProvider(true);
    capabilities.setDocumentHighlightProvider(true);
    capabilities.setSignatureHelpProvider(new SignatureHelpOptions(FunctionHelp.TRIGGERS));
    capabilities.setSemanticTokensProvider(
        new SemanticTokensWithRegistrationOptions(SemanticHighlighting.LEGEND, true));
    capabilities.setInlayHintProvider(true);
    capabilities.setCodeLensProvider(new CodeLensOptions(false));
    capabilities.setExecuteCommandProvider(
        new ExecuteCommandOptions(List.of(SqrlWorkspaceService.PACKAGE_FILES_COMMAND)));
    var client = Optional.ofNullable(params.getCapabilities());
    var workspace = client.map(ClientCapabilities::getWorkspace);
    // Semantic tokens, inlay hints and code lenses come from checks, which finish after the
    // editor asked for them
    checks.setRefresh(
        workspace
            .map(WorkspaceClientCapabilities::getSemanticTokens)
            .map(SemanticTokensWorkspaceCapabilities::getRefreshSupport)
            .orElse(false),
        workspace
            .map(WorkspaceClientCapabilities::getInlayHint)
            .map(InlayHintWorkspaceCapabilities::getRefreshSupport)
            .orElse(false),
        workspace
            .map(WorkspaceClientCapabilities::getCodeLens)
            .map(CodeLensWorkspaceCapabilities::getRefreshSupport)
            .orElse(false));
    textDocumentService.setParameterLabelOffsets(
        client
            .map(ClientCapabilities::getTextDocument)
            .map(TextDocumentClientCapabilities::getSignatureHelp)
            .map(SignatureHelpCapabilities::getSignatureInformation)
            .map(SignatureInformationCapabilities::getParameterInformation)
            .map(ParameterInformationCapabilities::getLabelOffsetSupport)
            .orElse(false));
    // The prepare request may only be announced to editors that support it
    var prepareRename =
        client
            .map(ClientCapabilities::getTextDocument)
            .map(TextDocumentClientCapabilities::getRename)
            .map(RenameCapabilities::getPrepareSupport)
            .orElse(false);
    capabilities.setRenameProvider(
        prepareRename ? Either.forRight(new RenameOptions(true)) : Either.forLeft(true));
    // Fixes are code actions, which editors without literal support would expect as commands
    var codeActionLiterals =
        client
            .map(ClientCapabilities::getTextDocument)
            .map(TextDocumentClientCapabilities::getCodeAction)
            .map(CodeActionCapabilities::getCodeActionLiteralSupport)
            .isPresent();
    if (codeActionLiterals) {
      capabilities.setCodeActionProvider(
          Either.forRight(new CodeActionOptions(List.of(CodeActionKind.QuickFix))));
    }
    var workspaceEdit = workspace.map(WorkspaceClientCapabilities::getWorkspaceEdit);
    textDocumentService.setChangeAnnotations(
        workspaceEdit.map(WorkspaceEditCapabilities::getDocumentChanges).orElse(false)
            && workspaceEdit
                .map(WorkspaceEditCapabilities::getChangeAnnotationSupport)
                .isPresent());

    var version = SqrlLanguageServer.class.getPackage().getImplementationVersion();
    return CompletableFuture.completedFuture(
        new InitializeResult(capabilities, new ServerInfo("sqrl", version)));
  }

  @Override
  public CompletableFuture<Object> shutdown() {
    shutdownRequested = true;
    return requests.answer(
        () -> {
          checks.close();
          return null;
        });
  }

  @Override
  public void exit() {
    requests.close();
    exitHandler.accept(shutdownRequested ? 0 : 1);
  }

  @Override
  public TextDocumentService getTextDocumentService() {
    return textDocumentService;
  }

  @Override
  public WorkspaceService getWorkspaceService() {
    return workspaceService;
  }
}
