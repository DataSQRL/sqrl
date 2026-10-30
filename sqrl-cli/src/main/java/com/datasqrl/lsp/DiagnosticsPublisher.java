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

import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.loaders.ModuleLoaderImpl;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.DiagnosticSeverity;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.PublishDiagnosticsParams;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.services.LanguageClient;

/**
 * Publishes the diagnostics of the checks of projects. Since a file can be used by several projects
 * (e.g. a shared package), each file gets the diagnostics of all projects, and files that had
 * diagnostics before but have none now are cleared. Messages about the configuration are put at the
 * value of the package file they are about. Besides the diagnostics of the compiler, the variables
 * of templates that the package files do not define get a warning, and open scripts that are not
 * part of a project get a warning that tells why they are not checked.
 *
 * <p>After a check, the editor is asked to refresh what depends on it, e.g. semantic tokens.
 */
@RequiredArgsConstructor
class DiagnosticsPublisher {

  private final OpenDocuments documents;
  private final Path workRoot;
  private volatile LanguageClient client;
  private volatile boolean closed = false;
  private volatile boolean refreshSemanticTokens = false;
  private volatile boolean refreshInlayHints = false;
  private volatile boolean refreshCodeLenses = false;

  /** The last published diagnostics of each project by file */
  private final Map<Path, Map<Path, List<Diagnostic>>> diagnosticsByProject =
      new ConcurrentHashMap<>();

  /** The open scripts that were told that they are not part of a project */
  private final Set<Path> withoutProject = ConcurrentHashMap.newKeySet();

  void connect(LanguageClient languageClient) {
    this.client = languageClient;
  }

  /** Whether the editor can be asked to refresh semantic tokens, inlay hints and code lenses. */
  void setRefresh(boolean semanticTokens, boolean inlayHints, boolean codeLenses) {
    this.refreshSemanticTokens = semanticTokens;
    this.refreshInlayHints = inlayHints;
    this.refreshCodeLenses = codeLenses;
  }

  void close() {
    closed = true;
  }

  /**
   * Publishes the diagnostics of a check of a project.
   *
   * @param packageFiles the package files of the project, the first of which gets the messages
   *     without a source file, e.g. about the configuration
   */
  void publish(Path project, List<Path> packageFiles, CheckResult result) {
    var byFile = new HashMap<Path, List<Diagnostic>>();
    for (var diagnostic : result.diagnostics()) {
      var file = diagnostic.file().filter(f -> !f.startsWith(workRoot));
      var converted =
          DiagnosticConverter.convert(diagnostic, file.flatMap(documents::read).orElse(null));
      // A message about the configuration goes to the value it is about, if it names it
      var located =
          diagnostic.range().isPresent()
              ? Optional.<JsonPointers.FileRange>empty()
              : JsonPointers.locate(
                  diagnostic.message(), file.map(List::of).orElse(packageFiles), documents::read);
      located.ifPresent(at -> converted.setRange(at.range()));
      byFile
          .computeIfAbsent(
              located.map(JsonPointers.FileRange::file).or(() -> file).orElse(packageFiles.get(0)),
              f -> new ArrayList<>())
          .add(converted);
    }
    for (var file : result.sourceFiles()) {
      var text =
          file.toString().endsWith(ModuleLoaderImpl.SQRL_FILE_EXTENSION)
              ? documents.read(file).orElse("")
              : "";
      if (text.contains("{{")) {
        var unknown =
            TemplateVariables.unknown(text, TemplateVariables.of(file, packageFiles)).stream()
                .map(DiagnosticsPublisher::unknownVariable)
                .toList();
        if (!unknown.isEmpty()) {
          byFile.computeIfAbsent(file, f -> new ArrayList<>()).addAll(unknown);
        }
      }
      // Clear the warning of scripts that turned out to be part of this project
      if (withoutProject.remove(file)) {
        byFile.putIfAbsent(file, List.of());
      }
    }
    publish(project, byFile);
    refresh();
  }

  /** Removes the diagnostics of a project. */
  void clear(Path project) {
    publish(project, Map.of());
  }

  /** Publishes that a project could not be checked at all. */
  void publishFailure(Path project, Path packageFile, Throwable e) {
    publish(
        project,
        Map.of(
            packageFile,
            List.of(
                diagnostic(
                    new Range(new Position(0, 0), new Position(0, 0)),
                    "The SQRL language server could not check this project: " + e,
                    DiagnosticSeverity.Error))));
  }

  /** Warns that an open script is not checked, or clears the warning if it is part of a project. */
  void projectFound(Path file, boolean found) {
    var languageClient = client;
    var unchecked =
        !found && OpenDocuments.isScript(file) && documents.snapshot().containsKey(file);
    if ((unchecked ? withoutProject.add(file) : withoutProject.remove(file))
        && languageClient != null) {
      var message =
          "This script is not checked: no package file in its directory or above names it as its"
              + " main script, e.g. a package.json with {\"script\": {\"main\": \"%s\"}}."
                  .formatted(file.getFileName());
      languageClient.publishDiagnostics(
          new PublishDiagnosticsParams(
              documents.uri(file),
              unchecked
                  ? List.of(
                      diagnostic(
                          new Range(new Position(0, 0), new Position(0, 0)),
                          message,
                          DiagnosticSeverity.Warning))
                  : List.of()));
    }
  }

  private synchronized void publish(Path project, Map<Path, List<Diagnostic>> byFile) {
    var previous = diagnosticsByProject.put(project, byFile);
    var files = new LinkedHashSet<>(byFile.keySet());
    if (previous != null) {
      files.addAll(previous.keySet());
    }
    var languageClient = client;
    if (languageClient == null || closed) {
      return;
    }
    for (var file : files) {
      var diagnostics = new LinkedHashSet<Diagnostic>();
      diagnosticsByProject
          .values()
          .forEach(m -> diagnostics.addAll(m.getOrDefault(file, List.of())));
      languageClient.publishDiagnostics(
          new PublishDiagnosticsParams(documents.uri(file), new ArrayList<>(diagnostics)));
    }
  }

  private void refresh() {
    var languageClient = client;
    if (languageClient == null || closed) {
      return;
    }
    if (refreshSemanticTokens) {
      languageClient.refreshSemanticTokens();
    }
    if (refreshInlayHints) {
      languageClient.refreshInlayHints();
    }
    if (refreshCodeLenses) {
      languageClient.refreshCodeLenses();
    }
  }

  private static Diagnostic unknownVariable(TemplateVariables.Tag tag) {
    return diagnostic(
        new Range(new Position(tag.line(), tag.start()), new Position(tag.line(), tag.end())),
        "`%s` is not defined in `script.config` of the package files, so it is replaced with nothing."
            .formatted(tag.name()),
        DiagnosticSeverity.Warning);
  }

  private static Diagnostic diagnostic(Range range, String message, DiagnosticSeverity severity) {
    return new Diagnostic(range, message, severity, DiagnosticConverter.SOURCE);
  }
}
