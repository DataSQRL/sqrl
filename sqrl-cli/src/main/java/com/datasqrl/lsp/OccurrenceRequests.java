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

import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.lsp.SymbolLookup.ColumnTarget;
import com.datasqrl.lsp.SymbolLookup.ImportedTableTarget;
import com.datasqrl.lsp.SymbolLookup.SymbolTarget;
import com.datasqrl.lsp.SymbolLookup.Target;
import com.datasqrl.lsp.SymbolOccurrences.Occurrence;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.eclipse.lsp4j.DocumentHighlight;
import org.eclipse.lsp4j.DocumentHighlightKind;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.Position;

/** Answers the requests for the occurrences of what a position in a script refers to. */
@RequiredArgsConstructor
class OccurrenceRequests {

  private final OpenDocuments documents;
  private final ProjectChecks checks;

  /**
   * The references of what a position refers to in the scripts of its projects, and of a table or
   * function in their GraphQL API.
   *
   * @param includeDeclaration whether to include the definitions
   */
  List<Location> references(Path file, String text, Position position, boolean includeDeclaration) {
    var projects = checks.symbolsByProjectOf(file);
    var indexes = List.copyOf(projects.values());
    var target =
        SymbolLookup.find(indexes, file, text, position.getLine(), position.getCharacter());
    if (target.isEmpty()) {
      return List.of();
    }
    var result = new LinkedHashSet<Location>();
    occurrences(indexes, target.get()).stream()
        .filter(o -> includeDeclaration || !o.definition())
        .forEach(o -> result.add(toLocation(o.range())));
    if (target.get() instanceof SymbolTarget symbol) {
      new SymbolRename(projects, documents::read)
          .findInApi(symbol.symbol())
          .forEach(found -> found.ranges().forEach(r -> result.add(toLocation(r))));
    }
    return List.copyOf(result);
  }

  /** The occurrences in the script of what a position refers to, the definitions as writes. */
  List<DocumentHighlight> highlights(Path file, String text, Position position) {
    var indexes = checks.symbolsOf(file);
    return SymbolLookup.find(indexes, file, text, position.getLine(), position.getCharacter())
        .map(
            target ->
                occurrences(indexes, target).stream()
                    .filter(o -> o.range().file().equals(file))
                    .map(
                        o ->
                            new DocumentHighlight(
                                toRange(o.range()),
                                o.definition()
                                    ? DocumentHighlightKind.Write
                                    : DocumentHighlightKind.Read))
                    .toList())
        .orElse(List.of());
  }

  private static List<Occurrence> occurrences(List<SymbolIndex> indexes, Target target) {
    var occurrences = new SymbolOccurrences(indexes);
    if (target instanceof SymbolTarget symbol) {
      return occurrences.of(symbol.symbol());
    } else if (target instanceof ColumnTarget column) {
      return occurrences.of(column.reference(), column.table());
    } else if (target instanceof ImportedTableTarget imported) {
      return occurrences.of(imported);
    }
    return List.of();
  }

  private Location toLocation(SourceRange range) {
    return new Location(documents.uri(range.file()), toRange(range));
  }
}
