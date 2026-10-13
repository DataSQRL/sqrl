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
package com.datasqrl.compile.check;

import static com.datasqrl.config.SqrlConstants.PACKAGE_JSON;

import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.CompilerContexts;
import com.datasqrl.compile.check.CheckResult.Diagnostic;
import com.datasqrl.compile.check.SymbolIndex.ApiFile;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.error.ErrorMessage.Severity;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.SourceFiles;
import com.datasqrl.plan.MainScript;
import com.datasqrl.planner.SqlScriptPlanner;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

/**
 * Checks a project for errors without generating deployment artifacts, for tools such as a language
 * server that check the same project repeatedly in one JVM.
 *
 * <p>All build output goes to the private work directory of the {@link CheckRequest}, so the
 * project directory is never modified. Unsaved content of source files can be passed as overlays.
 * Errors and symbols in build files are attributed to the source files they were created from.
 */
@Slf4j
public class ScriptChecker {

  public CheckResult check(CheckRequest request) {
    return check(request, () -> false);
  }

  /**
   * Checks a project, and stops early when a newer check replaces this one, e.g. after an edit.
   *
   * @param request what to check
   * @param outdated whether a newer check replaces this one, which stops planning at the next
   *     statement with a {@link CancellationException}
   */
  public CheckResult check(CheckRequest request, BooleanSupplier outdated) {
    var start = System.nanoTime();
    var errors = ErrorCollector.root();
    SourceFiles sourceFiles = null;
    var symbols = new SymbolCollector(outdated);
    var symbolIndex = SymbolIndex.EMPTY;
    Optional<PlannedCatalog> catalog = Optional.empty();
    try {
      var config = loadConfig(errors, request);
      var buildDir = request.workDir().resolve(SqrlConstants.BUILD_DIR_NAME);
      var dirs =
          new CompilerContexts.Directories(
              request.workDir(),
              request.projectRoot(),
              // Excluded from the sources, so that the build of the CLI is not copied
              request.projectRoot().resolve(SqrlConstants.BUILD_DIR_NAME),
              buildDir,
              buildDir.resolve(SqrlConstants.DEPLOY_DIR_NAME));

      try (var ctx = CompilerContexts.create(errors, dirs, config, request.goal(), false)) {
        sourceFiles = ctx.getBean(SourceFiles.class);
        sourceFiles.setOverlays(request.overlays());
        ctx.getBean(Packager.class)
            .preprocess(errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON)));
        if (!errors.hasErrors()) {
          ctx.getBean(SqlScriptPlanner.class).setListener(symbols);
          try {
            if (request.fullCompile()) {
              var testDir =
                  config
                      .getTestConfig()
                      .getTestDir(request.projectRoot())
                      .filter(Files::isDirectory);
              ctx.getBean(CompilationProcess.class).executeCompilation(testDir);
            } else {
              catalog = plan(ctx, config, request.keepCatalog());
            }
          } finally {
            symbolIndex =
                buildIndex(symbols, sourceFiles, apiFiles(config, request, buildDir, sourceFiles));
          }
        }
      }
    } catch (CancellationException e) {
      throw e;
    } catch (Throwable e) {
      if (!isCollected(e)) {
        log.warn("Unexpected error while checking {}", request.projectRoot(), e);
        errors.getCatcher().handle(e);
      }
    }
    var diagnostics = toDiagnostics(errors, sourceFiles, request);
    diagnostics.addAll(hintDiagnostics(symbols, sourceFiles));
    return new CheckResult(
        diagnostics,
        symbolIndex,
        sourceFiles == null ? Set.of() : usedFiles(sourceFiles, symbols),
        catalog,
        Duration.ofNanos(System.nanoTime() - start));
  }

  private static PackageJson loadConfig(ErrorCollector errors, CheckRequest request) {
    var configErrors = errors.withLocation(ErrorPrefix.CONFIG).resolve("package");
    return switch (request.goal()) {
      case COMPILE -> ConfigLoaderUtils.loadUnresolvedConfig(configErrors, request.packageFiles());
      case RUN -> ConfigLoaderUtils.loadUnresolvedRunConfig(configErrors, request.packageFiles());
      case TEST -> ConfigLoaderUtils.loadUnresolvedTestConfig(configErrors, request.packageFiles());
    };
  }

  /**
   * Plans the scripts, which reports their errors to the collector.
   *
   * @return the planned catalog, if it is kept
   */
  private static Optional<PlannedCatalog> plan(
      ApplicationContext ctx, PackageJson config, boolean keepCatalog) {
    var planner = ctx.getBean(SqlScriptPlanner.class);
    var env =
        new Sqrl2FlinkSQLTranslator(
            ctx.getBean(WorkspacePaths.class),
            (FlinkStreamEngine) planner.getStreamStage().engine(),
            config.getCompilerConfig());
    var catalog = Optional.<PlannedCatalog>empty();
    try {
      try {
        planner.planMain(ctx.getBean(MainScript.class), Optional.empty(), env);
      } catch (CollectedException e) {
        // The errors are in the collector
      }
      if (keepCatalog) {
        catalog = Optional.of(new PlannedCatalog(env));
      }
      return catalog;
    } finally {
      if (catalog.isEmpty()) {
        env.close();
      }
    }
  }

  /** Builds the index of what was planned, also when planning failed. */
  private static SymbolIndex buildIndex(
      SymbolCollector symbols, SourceFiles sourceFiles, List<ApiFile> apiFiles) {
    try {
      return symbols.build(sourceFiles, apiFiles);
    } catch (RuntimeException e) {
      log.warn("Could not index the symbols", e);
      return SymbolIndex.EMPTY;
    }
  }

  /**
   * The source files of the GraphQL schemas and operations that the package configures. Their paths
   * are relative to the build directory, which mirrors the project directory.
   */
  private static List<ApiFile> apiFiles(
      PackageJson config, CheckRequest request, Path buildDir, SourceFiles sourceFiles) {
    var files = new LinkedHashMap<Path, Boolean>();
    Consumer<String> addSchema =
        path -> sourceFile(path, request, buildDir, sourceFiles).ifPresent(f -> files.put(f, true));
    Consumer<String> addOperations =
        path ->
            sourceFile(path, request, buildDir, sourceFiles)
                .ifPresent(f -> files.putIfAbsent(f, false));
    try {
      var script = config.getScriptConfig();
      script.getGraphql().ifPresent(addSchema);
      script.getOperations().forEach(addOperations);
      for (var api : script.getScriptApiConfigs()) {
        api.getSchema().ifPresent(addSchema);
        api.getOperations().forEach(addOperations);
      }
    } catch (RuntimeException e) {
      log.debug("Could not read the API configuration", e);
    }
    return files.entrySet().stream().map(e -> new ApiFile(e.getKey(), e.getValue())).toList();
  }

  private static Optional<Path> sourceFile(
      String path, CheckRequest request, Path buildDir, SourceFiles sourceFiles) {
    try {
      return sourceFiles
          .getSourceFile(buildDir.resolve(path))
          .or(
              () ->
                  Optional.of(request.projectRoot().resolve(path).normalize())
                      .filter(Files::isRegularFile));
    } catch (InvalidPathException e) {
      return Optional.empty();
    }
  }

  private static List<Diagnostic> toDiagnostics(
      ErrorCollector errors, SourceFiles sourceFiles, CheckRequest request) {
    var diagnostics = new ArrayList<Diagnostic>();
    var positions = sourceFiles == null ? null : new BuildPositions(sourceFiles);
    for (ErrorMessage message : errors) {
      var location = message.getLocation();
      var file =
          location
              .getSourcePath()
              .map(Path::of)
              .map(
                  path ->
                      sourceFiles == null ? path : sourceFiles.getSourceFile(path).orElse(path));
      if (file.isEmpty() && isConfigLocation(location) && !request.packageFiles().isEmpty()) {
        file = Optional.of(request.packageFiles().get(0));
      }
      var range =
          location.hasFile()
              ? Optional.of(
                  positions == null || location.getSourcePath().isEmpty()
                      ? location.getFile()
                      : positions.toSource(
                          Path.of(location.getSourcePath().get()), location.getFile()))
              : Optional.<ErrorLocation.FileRange>empty();
      diagnostics.add(
          new Diagnostic(
              message.getSeverity(),
              formatMessage(message),
              file,
              range,
              message.getErrorLabel().getLabel()));
    }
    return diagnostics;
  }

  /**
   * The source files that a project uses: the scripts that were planned, i.e. its main script and
   * the scripts that it imports, and all other files, e.g. data and schemas. Other scripts in the
   * directories of the project, e.g. the main scripts of other projects, are not part of it.
   */
  private static Set<Path> usedFiles(SourceFiles sourceFiles, SymbolCollector symbols) {
    var planned = new HashSet<Path>();
    symbols
        .plannedFiles()
        .forEach(file -> planned.add(sourceFiles.getSourceFile(file).orElse(file)));
    var used = new LinkedHashSet<Path>();
    for (var file : sourceFiles.getSourceFiles()) {
      if (!file.toString().endsWith(ModuleLoaderImpl.SQRL_FILE_EXTENSION)
          || planned.contains(file)) {
        used.add(file);
      }
    }
    return used;
  }

  /** The options of hints in queries that Flink would reject, see {@link QueryHints}. */
  private static List<Diagnostic> hintDiagnostics(
      SymbolCollector symbols, SourceFiles sourceFiles) {
    if (sourceFiles == null) {
      return List.of();
    }
    var positions = new BuildPositions(sourceFiles);
    return symbols.hintProblems().stream()
        .map(
            problem ->
                new Diagnostic(
                    Severity.FATAL,
                    problem.message(),
                    sourceFiles
                        .getSourceFile(problem.buildFile())
                        .or(() -> Optional.of(problem.buildFile())),
                    Optional.of(positions.toSource(problem.buildFile(), problem.range())),
                    ErrorLabel.GENERIC.getLabel()))
        .toList();
  }

  private static boolean isConfigLocation(ErrorLocation location) {
    return ErrorPrefix.CONFIG.getPrefix().equals(location.getPrefix());
  }

  private static String formatMessage(ErrorMessage message) {
    var text = message.getMessage() == null ? "" : message.getMessage().strip();
    var prefix = message.getMessagePrefix();
    return prefix == null || prefix.isBlank() ? text : prefix.strip() + ": " + text;
  }

  private static boolean isCollected(Throwable e) {
    for (var cause = e;
        cause != null;
        cause = cause.getCause() == cause ? null : cause.getCause()) {
      if (cause instanceof CollectedException) {
        return true;
      }
    }
    return false;
  }
}
