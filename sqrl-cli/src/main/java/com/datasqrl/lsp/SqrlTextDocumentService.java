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

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.lsp.SymbolLookup.Target;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.lsp4j.CodeAction;
import org.eclipse.lsp4j.CodeActionKind;
import org.eclipse.lsp4j.CodeActionParams;
import org.eclipse.lsp4j.CodeLens;
import org.eclipse.lsp4j.CodeLensParams;
import org.eclipse.lsp4j.Command;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionList;
import org.eclipse.lsp4j.CompletionParams;
import org.eclipse.lsp4j.DefinitionParams;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidCloseTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.DidSaveTextDocumentParams;
import org.eclipse.lsp4j.DocumentFormattingParams;
import org.eclipse.lsp4j.DocumentHighlight;
import org.eclipse.lsp4j.DocumentHighlightParams;
import org.eclipse.lsp4j.DocumentSymbol;
import org.eclipse.lsp4j.DocumentSymbolParams;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.HoverParams;
import org.eclipse.lsp4j.InlayHint;
import org.eclipse.lsp4j.InlayHintParams;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.LocationLink;
import org.eclipse.lsp4j.MarkupContent;
import org.eclipse.lsp4j.MarkupKind;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.PrepareRenameDefaultBehavior;
import org.eclipse.lsp4j.PrepareRenameParams;
import org.eclipse.lsp4j.PrepareRenameResult;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.ReferenceParams;
import org.eclipse.lsp4j.RenameParams;
import org.eclipse.lsp4j.SemanticTokens;
import org.eclipse.lsp4j.SemanticTokensParams;
import org.eclipse.lsp4j.SignatureHelp;
import org.eclipse.lsp4j.SignatureHelpParams;
import org.eclipse.lsp4j.SymbolInformation;
import org.eclipse.lsp4j.TextDocumentPositionParams;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.WorkspaceEdit;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.jsonrpc.messages.Either3;
import org.eclipse.lsp4j.services.LanguageClient;
import org.eclipse.lsp4j.services.TextDocumentService;

/**
 * Tracks the open documents and checks their projects when they change, and answers requests about
 * their content from the symbols and catalogs of the last checks. Renames are answered by {@link
 * RenameRequests}. All messages are handled on the {@link RequestThread}, in the order they arrive.
 */
@Slf4j
class SqrlTextDocumentService implements TextDocumentService {

  /**
   * Complete hints after typing {@code /*+}, and their arguments after {@code (} and {@code ,},
   * which are ignored outside of hints
   */
  static final List<String> HINT_TRIGGERS = List.of("+", "(", ",");

  /** Completes the variables of templates after typing {@code {{} */
  static final String TEMPLATE_TRIGGER = "{";

  private final OpenDocuments documents;
  private final ProjectChecks checks;
  private final RequestThread requests;
  private final RenameRequests renames;
  private final Definitions definitions;
  private final OccurrenceRequests occurrences;
  private volatile boolean parameterLabelOffsets = false;

  SqrlTextDocumentService(OpenDocuments documents, ProjectChecks checks, RequestThread requests) {
    this.documents = documents;
    this.checks = checks;
    this.requests = requests;
    this.renames = new RenameRequests(documents, checks);
    this.definitions = new Definitions(documents, checks);
    this.occurrences = new OccurrenceRequests(documents, checks);
  }

  void connect(LanguageClient languageClient) {
    renames.connect(languageClient);
  }

  /** Whether the editor can ask the user to confirm the annotated edits of a workspace edit. */
  void setChangeAnnotations(boolean supported) {
    renames.setChangeAnnotations(supported);
  }

  /** Whether the editor can find the parameters of a signature by their offsets in it. */
  void setParameterLabelOffsets(boolean supported) {
    this.parameterLabelOffsets = supported;
  }

  @Override
  public void didOpen(DidOpenTextDocumentParams params) {
    var document = params.getTextDocument();
    requests.handle(
        () ->
            OpenDocuments.toPath(document.getUri())
                .ifPresent(
                    file -> {
                      documents.open(file, document.getUri(), document.getText());
                      checks.fileChanged(file, false);
                    }));
  }

  @Override
  public void didChange(DidChangeTextDocumentParams params) {
    var changes = params.getContentChanges();
    if (changes.isEmpty()) {
      return;
    }
    // The server asks for full document sync, so the last change has the whole content
    var text = changes.get(changes.size() - 1).getText();
    requests.handle(
        () ->
            OpenDocuments.toPath(params.getTextDocument().getUri())
                .ifPresent(
                    file -> {
                      documents.update(file, text);
                      checks.fileChanged(file, false);
                    }));
  }

  @Override
  public void didClose(DidCloseTextDocumentParams params) {
    requests.handle(
        () ->
            OpenDocuments.toPath(params.getTextDocument().getUri())
                .ifPresent(
                    file -> {
                      documents.close(file);
                      checks.fileClosed(file);
                    }));
  }

  @Override
  public void didSave(DidSaveTextDocumentParams params) {
    requests.handle(
        () ->
            OpenDocuments.toPath(params.getTextDocument().getUri())
                .ifPresent(
                    file -> checks.fileChanged(file, checks.getSettings().fullCompileOnSave())));
  }

  @Override
  public CompletableFuture<List<Either<SymbolInformation, DocumentSymbol>>> documentSymbol(
      DocumentSymbolParams params) {
    return requests.answer(
        () ->
            withDocument(
                    params.getTextDocument().getUri(),
                    (file, text) ->
                        Optional.of(
                            DocumentOutline.of(text, file, checks.symbolsOf(file)).stream()
                                .map(Either::<SymbolInformation, DocumentSymbol>forRight)
                                .toList()))
                .orElse(List.of()));
  }

  @Override
  public CompletableFuture<List<? extends TextEdit>> formatting(DocumentFormattingParams params) {
    return requests.answer(
        () ->
            withDocument(
                    params.getTextDocument().getUri(),
                    (file, text) -> Optional.of(ScriptFormatter.edits(text, params.getOptions())))
                .orElse(List.of()));
  }

  @Override
  public CompletableFuture<Either<List<CompletionItem>, CompletionList>> completion(
      CompletionParams params) {
    return requests.answer(() -> complete(params));
  }

  private Either<List<CompletionItem>, CompletionList> complete(CompletionParams params) {
    var position = params.getPosition();
    var trigger = params.getContext() == null ? null : params.getContext().getTriggerCharacter();
    var items =
        withDocument(
            params.getTextDocument().getUri(),
            (file, text) -> {
              var offset = new ScriptText(text).offset(position.getLine(), position.getCharacter());
              if (trigger != null
                  && HINT_TRIGGERS.contains(trigger)
                  && !CompletionContext.isInHint(text, offset)) {
                return Optional.of(List.<CompletionItem>of());
              }
              var context = CompletionContext.at(text, offset);
              if (context.kind() == CompletionContext.Kind.TEMPLATE) {
                return Optional.of(TemplateVariables.complete(variables(file)));
              } else if (TEMPLATE_TRIGGER.equals(trigger)) {
                return Optional.of(List.<CompletionItem>of());
              }
              var completion =
                  new ScriptCompletion(
                      file,
                      checks.catalogOf(file),
                      checks.symbolsOf(file),
                      checks.buildDirOf(file));
              return Optional.of(completion.complete(context));
            });
    return Either.forRight(new CompletionList(false, items.orElse(List.of())));
  }

  @Override
  public CompletableFuture<Hover> hover(HoverParams params) {
    return requests.answer(() -> hoverOf(params));
  }

  private Hover hoverOf(HoverParams params) {
    // A template variable that stands for a name, e.g. a column, also shows what the name is
    var template = templateHover(params);
    var symbol = find(params).flatMap(HoverContent::render);
    var hover =
        template
            .map(value -> symbol.map(name -> value + "\n\n---\n\n" + name).orElse(value))
            .or(() -> symbol)
            .or(
                () ->
                    withFunctions(
                        params,
                        (functions, text, offset) ->
                            functions
                                .hover(text, offset)
                                .or(() -> functions.importHover(text, offset))))
            .map(markdown -> new Hover(new MarkupContent(MarkupKind.MARKDOWN, markdown)));
    return hover.orElse(null);
  }

  @Override
  public CompletableFuture<SignatureHelp> signatureHelp(SignatureHelpParams params) {
    return requests.answer(
        () ->
            withFunctions(
                    params,
                    (functions, text, offset) ->
                        functions.signatureHelp(text, offset, parameterLabelOffsets))
                .orElse(null));
  }

  private interface FunctionRequest<T> {
    Optional<T> apply(FunctionHelp functions, String text, int offset);
  }

  private <T> Optional<T> withFunctions(
      TextDocumentPositionParams params, FunctionRequest<T> request) {
    var position = params.getPosition();
    return withDocument(
        params.getTextDocument().getUri(),
        (file, text) ->
            request.apply(
                new FunctionHelp(file, checks.catalogOf(file), checks.symbolsOf(file)),
                text,
                new ScriptText(text).offset(position.getLine(), position.getCharacter())));
  }

  @Override
  public CompletableFuture<List<? extends CodeLens>> codeLens(CodeLensParams params) {
    return requests.answerLater(
        () -> TestLenses.request(params.getTextDocument().getUri(), checks, documents));
  }

  @Override
  public CompletableFuture<SemanticTokens> semanticTokensFull(SemanticTokensParams params) {
    return requests.answer(
        () ->
            withDocument(
                    params.getTextDocument().getUri(),
                    (file, text) ->
                        Optional.of(SemanticHighlighting.of(file, text, checks.symbolsOf(file))))
                .orElse(new SemanticTokens(List.of())));
  }

  /**
   * Answers once the script was checked with its current content, since editors may keep the hints
   * until the script changes again, e.g. IntelliJ does when it is asked to refresh them.
   */
  @Override
  public CompletableFuture<List<InlayHint>> inlayHint(InlayHintParams params) {
    return requests.answerLater(() -> inlayHintsAfterCheck(params));
  }

  private CompletableFuture<List<InlayHint>> inlayHintsAfterCheck(InlayHintParams params) {
    var uri = params.getTextDocument().getUri();
    var stale =
        OpenDocuments.toPath(uri)
            .filter(OpenDocuments::isScript)
            .filter(
                file -> {
                  var text = documents.read(file).orElse(null);
                  var indexes = checks.symbolsOf(file);
                  return indexes.isEmpty()
                      || indexes.stream().anyMatch(index -> !index.isCurrent(file, text));
                });
    var checked =
        stale
            .map(file -> checks.afterScheduledChecks(file).completeOnTimeout(null, 10, SECONDS))
            .orElse(CompletableFuture.completedFuture(null));
    return checked.thenApplyAsync(
        ignored ->
            withDocument(
                    uri,
                    (file, text) ->
                        Optional.of(
                            ColumnTypeHints.of(
                                file, text, checks.symbolsOf(file), params.getRange())))
                .orElse(List.of()));
  }

  /** The value of the variable of a template at a position, e.g. of {@code {{variant}}}. */
  private Optional<String> templateHover(TextDocumentPositionParams params) {
    var position = params.getPosition();
    return withDocument(
        params.getTextDocument().getUri(),
        (file, text) -> {
          var offset = new ScriptText(text).offset(position.getLine(), position.getCharacter());
          return TemplateVariables.nameAt(text, offset)
              .map(
                  name ->
                      Optional.ofNullable(variables(file).get(name))
                          .map(value -> "`%s` = `%s`".formatted(name, value))
                          .orElse(
                              "`%s` is not defined in `script.config` of the package files, so it is replaced with nothing."
                                  .formatted(name)));
        });
  }

  private Map<String, String> variables(Path file) {
    return TemplateVariables.of(file, checks.packageFilesOf(file));
  }

  @Override
  public CompletableFuture<Either<List<? extends Location>, List<? extends LocationLink>>>
      definition(DefinitionParams params) {
    return requests.answer(() -> Either.forLeft(definitions.of(find(params), params)));
  }

  @Override
  public CompletableFuture<List<? extends Location>> references(ReferenceParams params) {
    var includeDeclaration =
        params.getContext() == null || params.getContext().isIncludeDeclaration();
    return requests.answer(
        () ->
            withDocument(
                    params.getTextDocument().getUri(),
                    (file, text) ->
                        Optional.of(
                            occurrences.references(
                                file, text, params.getPosition(), includeDeclaration)))
                .orElse(List.of()));
  }

  @Override
  public CompletableFuture<List<? extends DocumentHighlight>> documentHighlight(
      DocumentHighlightParams params) {
    return requests.answer(
        () ->
            withDocument(
                    params.getTextDocument().getUri(),
                    (file, text) ->
                        Optional.of(occurrences.highlights(file, text, params.getPosition())))
                .orElse(List.of()));
  }

  @Override
  public CompletableFuture<List<Either<Command, CodeAction>>> codeAction(CodeActionParams params) {
    return requests.answer(() -> quickFixes(params));
  }

  private List<Either<Command, CodeAction>> quickFixes(CodeActionParams params) {
    var context = params.getContext();
    var only = context.getOnly();
    // Kinds are hierarchical, e.g. `quickfix` includes `quickfix.import`
    if (only != null
        && only.stream()
            .noneMatch(
                kind -> kind.isEmpty() || (CodeActionKind.QuickFix + ".").startsWith(kind + "."))) {
      return List.of();
    }
    var diagnostics =
        context.getDiagnostics().stream()
            .filter(d -> DiagnosticConverter.SOURCE.equals(d.getSource()))
            .toList();
    if (diagnostics.isEmpty()) {
      return List.of();
    }
    var uri = params.getTextDocument().getUri();
    var actions =
        withDocument(
            uri,
            (file, text) -> {
              var buildDir = checks.buildDirOf(file);
              var fixes =
                  new QuickFixes(
                      file,
                      uri,
                      text,
                      completionContext ->
                          new ScriptCompletion(
                                  file, checks.catalogOf(file), checks.symbolsOf(file), buildDir)
                              .complete(completionContext),
                      buildDir.map(dir -> new ImportCandidates(dir, file)));
              return Optional.of(
                  diagnostics.stream()
                      .flatMap(diagnostic -> fixes.fix(diagnostic).stream())
                      .map(Either::<Command, CodeAction>forRight)
                      .toList());
            });
    return actions.orElse(List.of());
  }

  @Override
  public CompletableFuture<Either3<Range, PrepareRenameResult, PrepareRenameDefaultBehavior>>
      prepareRename(PrepareRenameParams params) {
    return requests.answerLater(() -> renames.prepareRename(params));
  }

  @Override
  public CompletableFuture<WorkspaceEdit> rename(RenameParams params) {
    return requests.answerLater(() -> renames.rename(params));
  }

  private Optional<Target> find(TextDocumentPositionParams params) {
    var position = params.getPosition();
    return withDocument(
        params.getTextDocument().getUri(),
        (file, text) ->
            SymbolLookup.find(
                checks.symbolsOf(file), file, text, position.getLine(), position.getCharacter()));
  }

  static Range toRange(SourceRange range) {
    return new Range(
        new Position(range.startLine(), range.startCharacter()),
        new Position(range.endLine(), range.endCharacter()));
  }

  /**
   * Answers a request about a script with its current content. Other files, e.g. package files and
   * GraphQL schemas, are only opened for their diagnostics.
   */
  private <T> Optional<T> withDocument(String uri, BiFunction<Path, String, Optional<T>> request) {
    try {
      return OpenDocuments.toPath(uri)
          .filter(OpenDocuments::isScript)
          .flatMap(file -> documents.read(file).flatMap(text -> request.apply(file, text)));
    } catch (RuntimeException e) {
      log.warn("Could not answer a request about {}", uri, e);
      return Optional.empty();
    }
  }
}
