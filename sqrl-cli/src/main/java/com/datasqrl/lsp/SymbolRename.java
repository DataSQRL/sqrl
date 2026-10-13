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

import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_DATABASE;
import static com.datasqrl.lsp.RenameRequests.API_CHANGE;
import static com.datasqrl.lsp.SqrlTextDocumentService.toRange;
import static com.datasqrl.lsp.SymbolOccurrences.sameDefinition;
import static com.datasqrl.lsp.SymbolOccurrences.sameDefinitions;
import static com.datasqrl.lsp.SymbolOccurrences.simpleName;

import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.lsp.GraphqlNames.Names;
import com.datasqrl.lsp.GraphqlNames.TypeField;
import com.datasqrl.lsp.SymbolLookup.ColumnTarget;
import com.datasqrl.lsp.SymbolLookup.SymbolTarget;
import com.datasqrl.lsp.SymbolOccurrences.Occurrence;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.eclipse.lsp4j.AnnotatedTextEdit;
import org.eclipse.lsp4j.ChangeAnnotation;
import org.eclipse.lsp4j.ResourceOperation;
import org.eclipse.lsp4j.SnippetTextEdit;
import org.eclipse.lsp4j.TextDocumentEdit;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.VersionedTextDocumentIdentifier;
import org.eclipse.lsp4j.WorkspaceEdit;
import org.eclipse.lsp4j.jsonrpc.messages.Either;

/**
 * Renames a table, function or relationship wherever the checked projects name it: in the scripts,
 * and in the GraphQL schemas and operations of the projects whose API it is part of. Columns cannot
 * be renamed yet, since a renamed column would also have to be renamed in the tables that select it
 * with {@code *}.
 *
 * <p>The positions of the last checks must be valid for the current content of all files that are
 * changed, so a rename is refused while a file has changes that are not checked yet.
 */
class SymbolRename {

  /** Why a rename is not possible, in words for the user. */
  static class RenameException extends Exception {

    RenameException(String message) {
      super(message);
    }
  }

  /** A rename is not possible yet, since a file changed after the last check. */
  static final class NotCheckedException extends RenameException {

    NotCheckedException(Path file) {
      super(
          "`%s` changed since it was last checked, try again when the check is done."
              .formatted(file.getFileName()));
    }
  }

  /**
   * What to rename.
   *
   * @param symbol the table, function or relationship
   * @param range its name at the position of the request
   */
  record Target(Symbol symbol, SourceRange range) {}

  /**
   * A name to replace.
   *
   * @param range the name
   * @param api whether it is in a GraphQL file, which changes the API
   */
  record Edit(SourceRange range, boolean api, boolean doubleQuoted) {

    Edit(SourceRange range, boolean api) {
      this(range, api, false);
    }

    /** The new name, in double quotes where the name was, e.g. in the SQL of a database. */
    String newText(String newName) {
      return doubleQuoted ? '"' + newName + '"' : newName;
    }
  }

  /**
   * The edits of a rename.
   *
   * @param oldName the name that is replaced
   * @param newName the new name
   * @param edits the names to replace
   * @param projects the projects that use the renamed symbol
   */
  record Plan(String oldName, String newName, List<Edit> edits, Set<Path> projects) {

    boolean changesApi() {
      return edits.stream().anyMatch(Edit::api);
    }

    List<Path> apiFiles() {
      return edits.stream().filter(Edit::api).map(e -> e.range().file()).distinct().toList();
    }

    /** How the rename changes the API, for the user to confirm it. */
    String describeApiChange() {
      var files =
          apiFiles().stream()
              .map(file -> "`" + file.getFileName() + "`")
              .collect(Collectors.joining(", "));
      return "Renaming `%s` to `%s` also renames it in the GraphQL API (%s), so clients of the API need to use the new name."
          .formatted(oldName, newName, files);
    }

    /**
     * The edits for the editor.
     *
     * @param uris the URIs of files
     * @param changeAnnotations whether the editor asks the user to confirm annotated edits, in
     *     which case the edits of the API are annotated
     */
    WorkspaceEdit toWorkspaceEdit(Function<Path, String> uris, boolean changeAnnotations) {
      var byFile = new LinkedHashMap<Path, List<Edit>>();
      edits.forEach(e -> byFile.computeIfAbsent(e.range().file(), f -> new ArrayList<>()).add(e));
      if (!changeAnnotations) {
        var changes = new LinkedHashMap<String, List<TextEdit>>();
        byFile.forEach(
            (file, fileEdits) ->
                changes.put(
                    uris.apply(file),
                    fileEdits.stream()
                        .map(e -> new TextEdit(toRange(e.range()), e.newText(newName)))
                        .toList()));
        return new WorkspaceEdit(changes);
      }
      var documentChanges = new ArrayList<Either<TextDocumentEdit, ResourceOperation>>();
      byFile.forEach(
          (file, fileEdits) ->
              documentChanges.add(
                  Either.forLeft(
                      new TextDocumentEdit(
                          new VersionedTextDocumentIdentifier(uris.apply(file), null),
                          fileEdits.stream()
                              .map(
                                  e ->
                                      Either.<TextEdit, SnippetTextEdit>forLeft(
                                          e.api()
                                              ? new AnnotatedTextEdit(
                                                  toRange(e.range()), newName, API_CHANGE)
                                              : new TextEdit(
                                                  toRange(e.range()), e.newText(newName))))
                              .toList()))));
      var edit = new WorkspaceEdit(documentChanges);
      if (changesApi()) {
        var annotation = new ChangeAnnotation("Rename in the GraphQL API");
        annotation.setNeedsConfirmation(true);
        annotation.setDescription(describeApiChange());
        edit.setChangeAnnotations(Map.of(API_CHANGE, annotation));
      }
      return edit;
    }
  }

  private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private final Map<Path, SymbolIndex> indexesByProject;
  private final List<SymbolIndex> indexes;
  private final Function<Path, Optional<String>> read;

  /**
   * @param indexesByProject the symbols of the last checks of the projects that use the file
   * @param read reads the current content of a file
   */
  SymbolRename(Map<Path, SymbolIndex> indexesByProject, Function<Path, Optional<String>> read) {
    this.indexesByProject = indexesByProject;
    this.indexes = List.copyOf(indexesByProject.values());
    this.read = read;
  }

  /** Finds what to rename at a position. */
  Target prepare(Path file, String text, int line, int character) throws RenameException {
    if (indexes.stream().noneMatch(index -> index.isCurrent(file, text))) {
      throw notChecked(file);
    }
    var found = SymbolLookup.find(indexes, file, text, line, character);
    if (found.isPresent() && found.get() instanceof ColumnTarget) {
      throw new RenameException("Columns cannot be renamed yet.");
    }
    if (found.isEmpty() || !(found.get() instanceof SymbolTarget target)) {
      throw new RenameException("Only tables, functions and relationships can be renamed.");
    }
    var symbol = target.symbol();
    var definitionFile = symbol.location().file().getFileName().toString();
    if (!definitionFile.endsWith(".sqrl")) {
      throw new RenameException(
          "`%s` is named after the file `%s`, rename the file instead."
              .formatted(simpleName(symbol), definitionFile));
    }
    var range =
        new SymbolOccurrences(indexes)
            .of(symbol).stream()
                .map(Occurrence::range)
                .filter(r -> r.contains(file, line, character))
                .findFirst()
                .orElseThrow(
                    () ->
                        new RenameException(
                            "Place the cursor on the name `%s` to rename it."
                                .formatted(simpleName(symbol))));
    return new Target(symbol, range);
  }

  /** Plans the edits that rename a table, function or relationship. */
  Plan plan(Target target, String newName) throws RenameException {
    var symbol = target.symbol();
    var oldName = simpleName(symbol);
    if (!NAME.matcher(newName).matches()) {
      throw new RenameException(
          "`%s` is not a valid name: use letters, digits and `_`, starting with a letter or `_`."
              .formatted(newName));
    }
    if (ScriptCompletion.needsQuotes(newName)) {
      throw new RenameException("`%s` is a reserved word in SQL.".formatted(newName));
    }
    var projects = new LinkedHashSet<Path>();
    indexesByProject.forEach(
        (project, index) -> {
          if (!sameDefinitions(index, symbol).isEmpty()) {
            projects.add(project);
          }
        });
    if (newName.equals(oldName)) {
      return new Plan(oldName, newName, List.of(), projects);
    }
    checkConflicts(symbol, newName);

    var edits = new ArrayList<Edit>();
    for (var occurrence : new SymbolOccurrences(indexes).of(symbol)) {
      var range = occurrence.range();
      checkCurrent(range.file());
      var text = new ScriptText(read.apply(range.file()).orElse(""));
      var start = text.offset(range.startLine(), range.startCharacter());
      edits.add(
          new Edit(range, false, start < text.text().length() && text.text().charAt(start) == '"'));
    }
    for (var found : findInApi(symbol)) {
      if (!found.unreadable().isEmpty()) {
        throw new RenameException(
            "The GraphQL file `%s` cannot be parsed, so `%s` cannot be renamed in the API."
                .formatted(found.unreadable().get(0).getFileName(), oldName));
      }
      if (found.namedType().isPresent()
          && !newName.equalsIgnoreCase(oldName)
          && found.hasType(newName)) {
        throw new RenameException(
            "The GraphQL schema already has a type named `%s`.".formatted(newName));
      }
      found.ranges().forEach(range -> edits.add(new Edit(range, true)));
    }
    return new Plan(oldName, newName, List.copyOf(new LinkedHashSet<>(edits)), projects);
  }

  /** Where the GraphQL files of the projects name a table, function or relationship. */
  List<GraphqlNames.Found> findInApi(Symbol symbol) {
    var result = new ArrayList<GraphqlNames.Found>();
    for (var index : indexes) {
      var names = apiNames(sameDefinitions(index, symbol));
      if (!names.isEmpty() && !index.apiFiles().isEmpty()) {
        result.add(GraphqlNames.find(names, index.apiFiles(), read));
      }
    }
    return result;
  }

  /** The content of the changed files after a rename. */
  Map<Path, String> apply(Plan plan) throws RenameException {
    var byFile = new LinkedHashMap<Path, List<SourceRange>>();
    plan.edits()
        .forEach(
            e -> byFile.computeIfAbsent(e.range().file(), f -> new ArrayList<>()).add(e.range()));
    var result = new HashMap<Path, String>();
    for (var entry : byFile.entrySet()) {
      var text = read.apply(entry.getKey()).orElseThrow(() -> notChecked(entry.getKey()));
      var script = new ScriptText(text);
      var renamed = new StringBuilder(text);
      var ranges = new ArrayList<>(entry.getValue());
      // From the end, so that the offsets of the other names stay valid
      ranges.sort(
          Comparator.comparingInt(
                  (SourceRange r) -> script.offset(r.startLine(), r.startCharacter()))
              .reversed());
      for (var range : ranges) {
        renamed.replace(
            script.offset(range.startLine(), range.startCharacter()),
            script.offset(range.endLine(), range.endCharacter()),
            plan.newName());
      }
      result.put(entry.getKey(), renamed.toString());
    }
    return result;
  }

  /**
   * The names of a table, function or relationship of the main script in the API. Tables of other
   * scripts are not part of it.
   */
  private static Names apiNames(List<Symbol> definitions) {
    for (var definition : definitions) {
      if (!definition.identifier().getDatabaseName().equals(FLINK_DEFAULT_DATABASE)) {
        continue;
      }
      var name = simpleName(definition);
      if (definition.kind() == Kind.RELATIONSHIP) {
        var path = definition.name();
        var table = path.substring(0, path.lastIndexOf('.'));
        return new Names(
            Optional.empty(), Optional.empty(), Optional.of(new TypeField(table, name)));
      }
      return new Names(Optional.of(name), Optional.of(name), Optional.empty());
    }
    return Names.NONE;
  }

  private void checkConflicts(Symbol symbol, String newName) throws RenameException {
    for (var index : indexes) {
      for (var definition : sameDefinitions(index, symbol)) {
        var id = definition.identifier();
        var path = definition.name();
        var parent = path.contains(".") ? path.substring(0, path.lastIndexOf('.')) : null;
        var renamed = parent == null ? newName : parent + "." + newName;
        var existing =
            index.symbols().stream()
                .filter(s -> !sameDefinition(s, symbol))
                .filter(s -> sameDatabase(s.identifier(), id))
                .filter(s -> s.identifier().getObjectName().equalsIgnoreCase(renamed))
                .findFirst();
        if (existing.isPresent()) {
          var location = existing.get().location();
          throw new RenameException(
              "`%s` already exists in `%s` line %d."
                  .formatted(
                      existing.get().name(),
                      location.file().getFileName(),
                      location.startLine() + 1));
        }
        if (definition.kind() == Kind.RELATIONSHIP) {
          var table =
              index.symbol(ObjectIdentifier.of(id.getCatalogName(), id.getDatabaseName(), parent));
          if (table.flatMap(t -> t.column(newName)).isPresent()) {
            throw new RenameException(
                "`%s` already has a column named `%s`.".formatted(parent, newName));
          }
        }
      }
    }
  }

  private static boolean sameDatabase(ObjectIdentifier identifier, ObjectIdentifier other) {
    return identifier.getCatalogName().equals(other.getCatalogName())
        && identifier.getDatabaseName().equals(other.getDatabaseName());
  }

  /** Fails if a file changed since the last check, so that the positions of the index are stale. */
  private void checkCurrent(Path file) throws RenameException {
    var text = read.apply(file);
    var checked =
        indexes.stream()
            .map(index -> index.texts().get(file))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    if (text.isEmpty() || !checked.contains(text.get())) {
      throw notChecked(file);
    }
  }

  private static RenameException notChecked(Path file) {
    return new NotCheckedException(file);
  }
}
