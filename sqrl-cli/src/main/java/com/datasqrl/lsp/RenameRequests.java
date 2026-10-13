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

import static com.datasqrl.lsp.SqrlTextDocumentService.toRange;

import com.datasqrl.lsp.SymbolRename.NotCheckedException;
import com.datasqrl.lsp.SymbolRename.Plan;
import com.datasqrl.lsp.SymbolRename.RenameException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.lsp4j.MessageActionItem;
import org.eclipse.lsp4j.MessageType;
import org.eclipse.lsp4j.PrepareRenameDefaultBehavior;
import org.eclipse.lsp4j.PrepareRenameParams;
import org.eclipse.lsp4j.PrepareRenameResult;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.RenameParams;
import org.eclipse.lsp4j.ShowMessageRequestParams;
import org.eclipse.lsp4j.TextDocumentPositionParams;
import org.eclipse.lsp4j.WorkspaceEdit;
import org.eclipse.lsp4j.jsonrpc.ResponseErrorException;
import org.eclipse.lsp4j.jsonrpc.messages.Either3;
import org.eclipse.lsp4j.jsonrpc.messages.ResponseError;
import org.eclipse.lsp4j.jsonrpc.messages.ResponseErrorCode;
import org.eclipse.lsp4j.services.LanguageClient;

/**
 * Answers the rename requests of the editor with {@link SymbolRename}. A rename is checked with the
 * compiler before it is returned, and refused if it would add errors. Renaming a table, function or
 * relationship of the API also renames it in the GraphQL schemas and operations, which the user has
 * to confirm: editors that support change annotations ask for the annotated edits, other editors
 * are asked with a message before the edit is returned.
 *
 * <p>A rename needs the positions of a check of the current content of the files. If a file changed
 * since the last check, e.g. because the editor previews the new name in the file while it is
 * typed, the projects are checked right away and the request is answered after the check.
 */
@Slf4j
@RequiredArgsConstructor
class RenameRequests {

  /** The change annotation of the edits of a rename in the GraphQL API */
  static final String API_CHANGE = "api";

  /** The action that confirms a rename in the GraphQL API, for editors without annotations */
  static final String RENAME_ACTION = "Rename";

  private final OpenDocuments documents;
  private final ProjectChecks checks;
  private volatile LanguageClient client;
  private volatile boolean changeAnnotations = false;

  void connect(LanguageClient languageClient) {
    this.client = languageClient;
  }

  /** Whether the editor can ask the user to confirm the annotated edits of a workspace edit. */
  void setChangeAnnotations(boolean supported) {
    this.changeAnnotations = supported;
  }

  CompletableFuture<Either3<Range, PrepareRenameResult, PrepareRenameDefaultBehavior>>
      prepareRename(PrepareRenameParams params) {
    return afterCheck(
        params,
        file -> {
          var target =
              target(
                  new SymbolRename(checks.symbolsByProjectOf(file), documents::read), file, params);
          return Either3.forSecond(
              new PrepareRenameResult(
                  toRange(target.range()), SymbolOccurrences.simpleName(target.symbol())));
        });
  }

  CompletableFuture<WorkspaceEdit> rename(RenameParams params) {
    return afterCheck(
            params,
            file -> {
              var rename = new SymbolRename(checks.symbolsByProjectOf(file), documents::read);
              var plan = rename.plan(target(rename, file, params), params.getNewName());
              return new Planned(plan, plan.edits().isEmpty() ? Map.of() : rename.apply(plan));
            })
        .thenCompose(
            planned -> {
              var plan = planned.plan();
              if (plan.edits().isEmpty()) {
                return CompletableFuture.completedFuture(new WorkspaceEdit(Map.of()));
              }
              return checks
                  .findNewErrors(plan.projects(), planned.renamed(), plan.changesApi())
                  .thenCompose(
                      error -> {
                        if (error.isPresent()) {
                          throw new CompletionException(
                              error(
                                  "Renaming `%s` to `%s` would cause an error: %s"
                                      .formatted(plan.oldName(), plan.newName(), error.get())));
                        }
                        return confirm(plan);
                      })
                  .thenApply(
                      confirmed ->
                          confirmed
                              ? plan.toWorkspaceEdit(documents::uri, changeAnnotations)
                              : null);
            });
  }

  /** A rename and the new content of the renamed files. */
  private record Planned(Plan plan, Map<Path, String> renamed) {}

  /** Answers a request about a file, after a check of its current content. */
  private interface Request<T> {
    T answer(Path file) throws RenameException;
  }

  /**
   * Answers a request right away, or after a check if a file changed since the last one.
   *
   * @return the answer, or a {@link ResponseErrorException} with the reason it was refused
   */
  private <T> CompletableFuture<T> afterCheck(
      TextDocumentPositionParams params, Request<T> request) {
    var uri = params.getTextDocument().getUri();
    var file = OpenDocuments.toPath(uri);
    if (file.isEmpty() || !OpenDocuments.isScript(file.get())) {
      return CompletableFuture.failedFuture(
          error("Tables, functions and relationships can be renamed in .sqrl scripts."));
    }
    try {
      return CompletableFuture.completedFuture(request.answer(file.get()));
    } catch (NotCheckedException e) {
      return checks.checkNow(file.get()).thenApplyAsync(checked -> answer(request, file.get()));
    } catch (RenameException | RuntimeException e) {
      return CompletableFuture.failedFuture(toError(e, uri));
    }
  }

  private <T> T answer(Request<T> request, Path file) {
    try {
      return request.answer(file);
    } catch (NotCheckedException e) {
      var reason =
          checks.projectsOf(file).isEmpty()
              ? error(
                  "`%s` is not part of a project, see its warning.".formatted(file.getFileName()))
              : error(e.getMessage());
      throw new CompletionException(reason);
    } catch (RenameException | RuntimeException e) {
      throw new CompletionException(toError(e, file));
    }
  }

  private static ResponseErrorException toError(Exception e, Object file) {
    if (e instanceof RenameException) {
      return error(e.getMessage());
    }
    log.warn("Could not rename in {}", file, e);
    return error("Could not rename: " + e);
  }

  private SymbolRename.Target target(
      SymbolRename rename, Path file, TextDocumentPositionParams params) throws RenameException {
    var text = documents.read(file).orElseThrow(() -> new RenameException("Cannot read " + file));
    var position = params.getPosition();
    return rename.prepare(file, text, position.getLine(), position.getCharacter());
  }

  /**
   * Asks the user to confirm a rename that changes the API, unless the editor asks for the
   * annotated edits itself.
   */
  private CompletableFuture<Boolean> confirm(Plan plan) {
    if (!plan.changesApi() || changeAnnotations) {
      return CompletableFuture.completedFuture(true);
    }
    var languageClient = client;
    if (languageClient == null) {
      return CompletableFuture.completedFuture(false);
    }
    var request =
        new ShowMessageRequestParams(
            List.of(new MessageActionItem(RENAME_ACTION), new MessageActionItem("Cancel")));
    request.setType(MessageType.Warning);
    request.setMessage(plan.describeApiChange());
    return languageClient
        .showMessageRequest(request)
        .thenApply(action -> action != null && RENAME_ACTION.equals(action.getTitle()));
  }

  private static ResponseErrorException error(String message) {
    return new ResponseErrorException(
        new ResponseError(ResponseErrorCode.RequestFailed, message, null));
  }
}
