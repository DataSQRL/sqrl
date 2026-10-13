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

import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import org.eclipse.lsp4j.DidChangeConfigurationParams;
import org.eclipse.lsp4j.DidChangeWatchedFilesParams;
import org.eclipse.lsp4j.ExecuteCommandParams;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.SymbolInformation;
import org.eclipse.lsp4j.SymbolKind;
import org.eclipse.lsp4j.WorkspaceSymbol;
import org.eclipse.lsp4j.WorkspaceSymbolParams;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.services.WorkspaceService;

/**
 * Checks the projects of files that changed on disk, e.g. package configurations or data files,
 * when the editor watches them, and finds the tables, functions and relationships of all checked
 * projects by name.
 */
@RequiredArgsConstructor
class SqrlWorkspaceService implements WorkspaceService {

  /** The most symbols that a search returns */
  static final int MAX_SYMBOLS = 500;

  private final OpenDocuments documents;
  private final ProjectChecks checks;
  private final RequestThread requests;

  /**
   * The command that lists the package files of a script: {@code candidates}, the package files
   * that name the script as their main script, of which the user may select one, {@code overlays},
   * the overlays of the base in use, of which the user may select any, see {@link
   * LspSettings#selectedPackageFiles}, and {@code used}, the package files that it is checked with,
   * the base first. Its argument is the URI of the script, and the package files are URIs too.
   */
  static final String PACKAGE_FILES_COMMAND = "sqrl.packageFiles";

  /** Applies the settings that changed, which are given as in the initialization options. */
  @Override
  public void didChangeConfiguration(DidChangeConfigurationParams params) {
    requests.handle(() -> checks.reconfigure(checks.getSettings().with(params.getSettings())));
  }

  @Override
  public CompletableFuture<Object> executeCommand(ExecuteCommandParams params) {
    return requests.answer(() -> packageFiles(params));
  }

  private Object packageFiles(ExecuteCommandParams params) {
    if (!PACKAGE_FILES_COMMAND.equals(params.getCommand())
        || params.getArguments() == null
        || params.getArguments().isEmpty()) {
      return null;
    }
    var argument = params.getArguments().get(0);
    var uri =
        argument instanceof JsonPrimitive primitive ? primitive.getAsString() : argument.toString();
    var result = new JsonObject();
    OpenDocuments.toPath(uri)
        .ifPresent(
            script -> {
              var used = checks.packageFilesOfScript(script);
              result.add("candidates", uris(ProjectPackages.candidates(script)));
              result.add(
                  "overlays",
                  uris(used.isEmpty() ? List.of() : ProjectPackages.overlays(used.get(0))));
              result.add("used", uris(used));
            });
    return result;
  }

  private JsonArray uris(List<Path> files) {
    var array = new JsonArray();
    files.forEach(file -> array.add(documents.uri(file)));
    return array;
  }

  @Override
  public void didChangeWatchedFiles(DidChangeWatchedFilesParams params) {
    requests.handle(
        () -> {
          for (var change : params.getChanges()) {
            OpenDocuments.toPath(change.getUri())
                .ifPresent(file -> checks.fileChanged(file, false));
          }
        });
  }

  /**
   * The symbols whose names contain the characters of the query in the same order, ignoring case,
   * e.g. {@code CuTo} for {@code CustomerTotals}.
   */
  @Override
  public CompletableFuture<
          Either<List<? extends SymbolInformation>, List<? extends WorkspaceSymbol>>>
      symbol(WorkspaceSymbolParams params) {
    return requests.answer(() -> Either.forRight(symbols(params)));
  }

  private List<WorkspaceSymbol> symbols(WorkspaceSymbolParams params) {
    var query = params.getQuery() == null ? "" : params.getQuery().toLowerCase(Locale.ROOT);
    // A script can be part of several projects, e.g. a shared package
    var symbols = new LinkedHashMap<String, WorkspaceSymbol>();
    checks.allSymbols().stream()
        .flatMap(index -> index.symbols().stream())
        .filter(symbol -> matches(symbol.name().toLowerCase(Locale.ROOT), query))
        .sorted(Comparator.comparing(Symbol::name, String.CASE_INSENSITIVE_ORDER))
        .forEach(
            symbol -> symbols.putIfAbsent(symbol.name() + symbol.location(), toWorkspace(symbol)));
    return symbols.values().stream().limit(MAX_SYMBOLS).toList();
  }

  private static boolean matches(String name, String query) {
    var at = 0;
    for (var c : query.toCharArray()) {
      at = name.indexOf(c, at);
      if (at < 0) {
        return false;
      }
      at++;
    }
    return true;
  }

  private WorkspaceSymbol toWorkspace(Symbol symbol) {
    var kind =
        switch (symbol.kind()) {
          case TABLE -> SymbolKind.Class;
          case FUNCTION -> SymbolKind.Function;
          case RELATIONSHIP -> SymbolKind.Property;
        };
    var file = symbol.location().file();
    var location =
        new Location(documents.uri(file), SqrlTextDocumentService.toRange(symbol.location()));
    var result = new WorkspaceSymbol(symbol.name(), kind, Either.forLeft(location));
    result.setContainerName(
        symbol.kind() == Kind.TABLE
            ? file.getFileName().toString()
            : symbol.identifier().getDatabaseName());
    return result;
  }
}
