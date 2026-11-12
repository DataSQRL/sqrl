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
package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ScriptSqrlModule.ScriptNamespaceObject;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.ScriptStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Strings;

/**
 * How the own deployment of a standalone included project plans its scripts: the main script, and
 * the scripts that it imports, transitively. The deployment runs the statements of every script
 * that it plans, but only exposes the subscriptions of the scripts that it plans at the root, i.e.
 * the main script and the scripts that a script at the root imports inline ({@code IMPORT x.*}).
 */
@Slf4j
public final class IncludeDeployment {

  /** An included project whose deployment plans none of its scripts, e.g. without main script. */
  public static final IncludeDeployment NONE = new IncludeDeployment(Map.of());

  private static final Name STAR = Name.system("*");

  public enum ScriptPlanning {
    NOT_PLANNED,
    PLANNED,
    PLANNED_AT_ROOT;

    /** Whether the deployment runs the statements of the script, e.g. its EXPORT statements. */
    public boolean runsStatements() {
      return this != NOT_PLANNED;
    }

    /**
     * Whether the deployment exposes the subscriptions of the script, so it writes their topics.
     */
    public boolean exposesSubscriptions() {
      return this == PLANNED_AT_ROOT;
    }
  }

  private final Map<Path, ScriptPlanning> scripts;

  private IncludeDeployment(Map<Path, ScriptPlanning> scripts) {
    this.scripts = scripts;
  }

  public ScriptPlanning getPlanning(Path script) {
    return scripts.getOrDefault(normalize(script), ScriptPlanning.NOT_PLANNED);
  }

  /**
   * Follows the imports of the main script with the module loader of the included project, which
   * resolves them like the included project's own compilation. An import that does not resolve is
   * skipped, so its scripts count as not planned.
   *
   * @param includeLoader loads the modules of the included project from its root directory
   * @param mainScript the main script, relative to the root directory of the included project
   */
  static IncludeDeployment analyze(
      ModuleLoader includeLoader, String mainScript, SqrlStatementParser parser) {
    // A script module is named by its path without the file extension
    var mainModule =
        NamePath.of(
            Stream.of(Path.of(mainScript).normalize().toString().split("[/\\\\]"))
                .map(segment -> Strings.CS.removeEnd(segment, ModuleLoaderImpl.SQRL_FILE_EXTENSION))
                .toArray(String[]::new));
    var main =
        loadScripts(includeLoader, mainModule, STAR).stream()
            .filter(ScriptNamespaceObject::isInline)
            .findFirst();
    if (main.isEmpty()) {
      return NONE;
    }

    var scripts = new HashMap<Path, ScriptPlanning>();
    var queue = new ArrayDeque<PlannedScript>();
    queue.add(new PlannedScript(main.get(), ScriptPlanning.PLANNED_AT_ROOT));
    while (!queue.isEmpty()) {
      var planned = queue.poll();
      var script = planned.script().getScript();
      var path = normalize(script.getPath().orElseThrow());
      var previous = scripts.getOrDefault(path, ScriptPlanning.NOT_PLANNED);
      if (previous.compareTo(planned.planning()) >= 0) {
        continue;
      }
      scripts.put(path, planned.planning());

      for (var statement : parseStatements(parser, path, script.getContent())) {
        if (statement instanceof ParsedStatement parsed
            && parsed.statement().get() instanceof SqrlImportStatement importStatement) {
          var importPath = importStatement.getPackageIdentifier().get();
          var isRootImport =
              importPath.size() > 1
                  && importPath.getFirst().getDisplay().equals(ModuleLoaders.ROOT_PREFIX);
          var loader = isRootImport ? includeLoader : planned.script().getModuleLoader();
          var modulePath = isRootImport ? importPath.popFirst() : importPath;
          for (var imported : loadScripts(loader, modulePath.popLast(), modulePath.getLast())) {
            var atRoot = planned.planning().exposesSubscriptions() && imported.isInline();
            queue.add(
                new PlannedScript(
                    imported, atRoot ? ScriptPlanning.PLANNED_AT_ROOT : ScriptPlanning.PLANNED));
          }
        }
      }
    }
    return new IncludeDeployment(Map.copyOf(scripts));
  }

  /**
   * Parses the statements of a script of the included project, which reports the script's errors
   * itself, so a script that does not parse contributes no imports.
   */
  private static List<ScriptStatement> parseStatements(
      SqrlStatementParser parser, Path path, String content) {
    try {
      var errors = ErrorCollector.root().withScript(path, content);
      return parser.parseScript(content, errors).statements();
    } catch (RuntimeException e) {
      log.debug("Could not parse the script {} of an included project", path, e);
      return List.of();
    }
  }

  /** Loads the scripts that importing the given name of a module, or {@code *}, plans. */
  private static List<ScriptNamespaceObject> loadScripts(
      ModuleLoader loader, NamePath modulePath, Name name) {
    try {
      var module = loader.loadModule(modulePath);
      if (module.isEmpty()) {
        return List.of();
      }
      var objects =
          name.equals(STAR)
              ? module.get().getNamespaceObjects().stream()
              : module.get().getNamespaceObject(name).stream();
      return objects
          .filter(ScriptNamespaceObject.class::isInstance)
          .map(ScriptNamespaceObject.class::cast)
          .toList();
    } catch (RuntimeException e) {
      log.debug("Could not resolve the module {} of an included project", modulePath, e);
      return List.of();
    }
  }

  private static Path normalize(Path path) {
    return path.toAbsolutePath().normalize();
  }

  private record PlannedScript(ScriptNamespaceObject script, ScriptPlanning planning) {}
}
