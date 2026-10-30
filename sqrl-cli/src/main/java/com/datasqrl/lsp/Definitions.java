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

import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.lsp.SymbolLookup.ColumnTarget;
import com.datasqrl.lsp.SymbolLookup.DatabaseTarget;
import com.datasqrl.lsp.SymbolLookup.ImportTarget;
import com.datasqrl.lsp.SymbolLookup.ImportedTableTarget;
import com.datasqrl.lsp.SymbolLookup.SymbolTarget;
import com.datasqrl.lsp.SymbolLookup.Target;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.TextDocumentPositionParams;

/**
 * Where what is at a position of a script is defined: a table, a column, an imported script or
 * table, a database, or the Java source of an imported function.
 */
final class Definitions {

  private final OpenDocuments documents;
  private final ProjectChecks checks;

  Definitions(OpenDocuments documents, ProjectChecks checks) {
    this.documents = documents;
    this.checks = checks;
  }

  /** The definitions of a target, or else the source of the function an IMPORT imports. */
  List<Location> of(Optional<Target> target, TextDocumentPositionParams params) {
    var found = target.map(this::of).orElse(List.of());
    return found.isEmpty() ? importedFunction(params) : found;
  }

  private List<Location> of(Target target) {
    if (target instanceof SymbolTarget symbol) {
      return List.of(toLocation(symbol.symbol().location()));
    } else if (target instanceof ColumnTarget column && column.table().isPresent()) {
      // A column added by a separate statement, or else the table
      var table = column.table().get();
      var name = column.reference().column().get(0);
      var added =
          table.columnLocations().entrySet().stream()
              .filter(entry -> entry.getKey().equalsIgnoreCase(name))
              .map(entry -> entry.getValue())
              .findFirst();
      return List.of(toLocation(added.orElse(table.location())));
    } else if (target instanceof ImportTarget imports) {
      return imports.targets().stream().map(this::toLocation).toList();
    } else if (target instanceof ImportedTableTarget imported) {
      // The definition itself, for which editors show the occurrences
      return List.of(toLocation(imported.definition()));
    } else if (target instanceof DatabaseTarget database) {
      return database.scripts().stream().map(this::toLocation).toList();
    }
    return List.of();
  }

  /**
   * The Java source of the user-defined function that the IMPORT statement at a position imports,
   * or that is called at the position.
   */
  private List<Location> importedFunction(TextDocumentPositionParams params) {
    var file = OpenDocuments.toPath(params.getTextDocument().getUri());
    var text = file.flatMap(documents::read);
    var catalog = file.flatMap(checks::catalogOf);
    if (text.isEmpty() || catalog.isEmpty()) {
      return List.of();
    }
    var position = params.getPosition();
    var imported = FunctionSources.at(text.get(), position.getLine(), position.getCharacter());
    var name =
        imported
            .map(FunctionSources.Import::name)
            .or(
                () ->
                    FunctionSources.callAt(
                        text.get(), position.getLine(), position.getCharacter()));
    var className = name.flatMap(n -> catalog.get().functionClass(n));
    if (className.isEmpty()) {
      return List.of();
    }
    var projectDir =
        checks.packageFilesOf(file.get()).stream()
            .findFirst()
            .map(Path::getParent)
            .orElse(file.get().getParent());
    // A called function is found nearest to the directory that the script imports it from
    var importDir =
        imported
            .or(() -> FunctionSources.importOf(text.get(), name.get()))
            .map(i -> i.directory(projectDir))
            .orElse(projectDir);
    return FunctionSources.find(projectDir, importDir, className.get())
        .map(
            source -> {
              var at =
                  documents
                      .read(source)
                      .map(java -> FunctionSources.declaration(java, className.get()))
                      .orElse(new Position(0, 0));
              return new Location(documents.uri(source), new Range(at, at));
            })
        .stream()
        .toList();
  }

  private Location toLocation(SourceRange range) {
    return new Location(documents.uri(range.file()), SqrlTextDocumentService.toRange(range));
  }
}
