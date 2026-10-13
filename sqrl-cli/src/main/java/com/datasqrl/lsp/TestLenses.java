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

import com.datasqrl.compile.check.SymbolIndex;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.eclipse.lsp4j.CodeLens;
import org.eclipse.lsp4j.Command;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;

/**
 * The code lenses that run the tests of a project, above the test tables of a script, i.e. the ones
 * with the {@code test} hint.
 *
 * <p>Tests run in a terminal of the editor, so the editor handles the command {@link
 * #RUN_TESTS_COMMAND}: it runs {@code sqrl test} with the package files of the project, which are
 * its arguments as URIs, in the directory of the first one. The CLI runs all tests of a project.
 */
final class TestLenses {

  static final String RUN_TESTS_COMMAND = "sqrl.runTests";

  private TestLenses() {}

  /**
   * The code lenses of a script, once the check of its last edit finished: editors ask for them
   * right after an edit, and Neovim does not ask again when the server asks it to refresh them.
   */
  static CompletableFuture<List<? extends CodeLens>> request(
      String uri, ProjectChecks checks, OpenDocuments documents) {
    var file = OpenDocuments.toPath(uri).filter(OpenDocuments::isScript);
    if (file.isEmpty()) {
      return CompletableFuture.completedFuture(List.of());
    }
    return checks
        .afterScheduledChecks(file.get())
        .completeOnTimeout(null, 10, TimeUnit.SECONDS)
        .thenApplyAsync(
            checked ->
                documents
                    .read(file.get())
                    .map(
                        text ->
                            of(
                                file.get(),
                                text,
                                checks.symbolsOf(file.get()),
                                checks.packageFilesOf(file.get()),
                                documents::uri))
                    .orElse(List.of()));
  }

  /**
   * The code lenses of a script.
   *
   * @param file the script
   * @param text its current content
   * @param indexes the symbols of the projects that use the script
   * @param packageFiles the package files of the project of the script
   * @param uri the URI of a file, as the editor knows it
   */
  static List<CodeLens> of(
      Path file,
      String text,
      List<SymbolIndex> indexes,
      List<Path> packageFiles,
      Function<Path, String> uri) {
    if (packageFiles.isEmpty()) {
      return List.of();
    }
    var arguments = new ArrayList<Object>();
    packageFiles.forEach(packageFile -> arguments.add(uri.apply(packageFile)));
    var lines = new LinkedHashSet<Integer>();
    for (var index : indexes) {
      var checked = index.texts().get(file);
      if (checked == null) {
        continue;
      }
      var changed = ChangedLines.between(checked, text);
      for (var symbol : index.symbols()) {
        if (symbol.test() && symbol.location().file().equals(file)) {
          // On the line that the definition moved to since the check
          changed.line(symbol.location().startLine()).ifPresent(lines::add);
        }
      }
    }
    var lenses = new ArrayList<CodeLens>();
    for (var line : lines) {
      var command = new Command("▶ Run tests", RUN_TESTS_COMMAND, List.copyOf(arguments));
      lenses.add(
          new CodeLens(new Range(new Position(line, 0), new Position(line, 0)), command, null));
    }
    return lenses;
  }
}
