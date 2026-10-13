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

import com.datasqrl.compile.check.CheckRequest;
import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.compile.check.PlannedCatalog;
import com.datasqrl.compile.check.ScriptChecker;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.plan.validate.ExecutionGoal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.lsp4j.services.LanguageClient;

/**
 * Checks the projects of changed files, publishes their diagnostics and keeps their symbols and
 * planned catalogs. A project is identified by its first package file, see {@link ProjectPackages}.
 *
 * <p>Checks run one at a time on a single worker thread, a short delay after the last change of a
 * project, so that typing does not start a check per keystroke. A newer check of a project replaces
 * a running one, which stops at its next statement, so that a change does not wait for the check of
 * the previous one. The result of a check is dropped and the project checked again if one of its
 * files changed in the meantime without a newer check, e.g. a file not yet known to belong to it.
 */
@Slf4j
class ProjectChecks implements AutoCloseable {

  /**
   * A scheduled or running check of a project.
   *
   * @param outdated whether a newer check replaces it, which stops it if it runs
   */
  private record PendingCheck(
      ScheduledFuture<?> future, boolean fullCompile, AtomicBoolean outdated) {}

  private final ScriptChecker checker;
  private final ScheduledThreadPoolExecutor executor =
      new ScheduledThreadPoolExecutor(
          1,
          runnable -> {
            var thread = new Thread(runnable, "sqrl-lsp-check");
            thread.setDaemon(true);
            return thread;
          });
  private volatile boolean closed = false;
  private final OpenDocuments documents;
  private final Path workRoot;
  private volatile LspSettings settings = LspSettings.DEFAULT;
  private final DiagnosticsPublisher publisher;

  private final Map<Path, PendingCheck> pending = new HashMap<>();

  /** The requests that wait for the next check of each project, see {@link #checkNow} */
  private final Map<Path, List<CompletableFuture<Void>>> waiting = new HashMap<>();

  /** The package files of each project, by the first one, which identifies the project */
  private final Map<Path, List<Path>> packageFilesByProject = new ConcurrentHashMap<>();

  /** The projects that use a source file, from their last check */
  private final Map<Path, Set<Path>> projectsByFile = new ConcurrentHashMap<>();

  /** The symbols of each project from its last check */
  private final Map<Path, SymbolIndex> symbolsByProject = new ConcurrentHashMap<>();

  /** The planned catalog of each project from its last check that planned the scripts */
  private final Map<Path, PlannedCatalog> catalogsByProject = new ConcurrentHashMap<>();

  /** The projects that were dropped, whose running checks are not kept */
  private final Set<Path> dropped = ConcurrentHashMap.newKeySet();

  ProjectChecks(OpenDocuments documents, Path workRoot) {
    this(documents, workRoot, new ScriptChecker());
  }

  ProjectChecks(OpenDocuments documents, Path workRoot, ScriptChecker checker) {
    this.checker = checker;
    this.documents = documents;
    this.workRoot = workRoot;
    this.publisher = new DiagnosticsPublisher(documents, workRoot);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  void connect(LanguageClient languageClient) {
    publisher.connect(languageClient);
  }

  /** Whether the editor can be asked to refresh semantic tokens, inlay hints and code lenses. */
  void setRefresh(boolean semanticTokens, boolean inlayHints, boolean codeLenses) {
    publisher.setRefresh(semanticTokens, inlayHints, codeLenses);
  }

  void setSettings(LspSettings lspSettings) {
    this.settings = lspSettings;
  }

  LspSettings getSettings() {
    return settings;
  }

  /**
   * Applies changed settings, e.g. the package file that the user selected for a script, and checks
   * the open files again. Projects that no open file belongs to anymore are dropped with their
   * diagnostics.
   */
  synchronized void reconfigure(LspSettings changed) {
    var open = documents.snapshot().keySet();
    var before = open.stream().flatMap(file -> findProjectRoot(file).stream()).toList();
    settings = changed;
    var after = open.stream().flatMap(file -> findProjectRoot(file).stream()).toList();
    before.stream().filter(project -> !after.contains(project)).forEach(this::forget);
    open.forEach(file -> fileChanged(file, false));
  }

  private void forget(Path project) {
    dropped.add(project);
    var scheduled = pending.remove(project);
    if (scheduled != null) {
      scheduled.future().cancel(false);
    }
    projectsByFile.values().forEach(projects -> projects.remove(project));
    symbolsByProject.remove(project);
    var catalog = catalogsByProject.remove(project);
    if (catalog != null) {
      catalog.close();
    }
    publisher.clear(project);
    checked(project);
  }

  /** The package files of a script: of the project selected or found for it. */
  List<Path> packageFilesOfScript(Path script) {
    return findProjectRoot(script).map(this::packageFiles).orElse(List.of());
  }

  /**
   * Forgets the projects of a closed file that no open document uses any more, with their catalogs,
   * symbols and diagnostics, and checks the others again with the content of the file on disk.
   */
  synchronized void fileClosed(Path file) {
    var open = documents.snapshot().keySet();
    for (var project : projectsOf(file)) {
      if (open.stream().anyMatch(other -> projectsOf(other).contains(project))) {
        schedule(project, false);
      } else {
        forget(project);
      }
    }
    publisher.projectFound(file, true);
  }

  /** Schedules a check of all projects that use the file. */
  void fileChanged(Path file, boolean fullCompile) {
    var projects = projectsOf(file);
    for (var project : projects) {
      schedule(project, fullCompile);
    }
    publisher.projectFound(file, !projects.isEmpty());
  }

  /** The symbols of all projects, from their last checks. */
  Collection<SymbolIndex> allSymbols() {
    return symbolsByProject.values();
  }

  /** The symbols of the projects that use a file, from their last checks. */
  List<SymbolIndex> symbolsOf(Path file) {
    return projectsOf(file).stream().map(symbolsByProject::get).filter(Objects::nonNull).toList();
  }

  /** The symbols of the projects that use a file, from their last checks, by project. */
  Map<Path, SymbolIndex> symbolsByProjectOf(Path file) {
    var result = new LinkedHashMap<Path, SymbolIndex>();
    for (var project : projectsOf(file)) {
      var symbols = symbolsByProject.get(project);
      if (symbols != null) {
        result.put(project, symbols);
      }
    }
    return result;
  }

  /**
   * Checks on the check thread whether changing the content of files would add errors to projects,
   * without publishing anything. The scripts are checked as usual, and if the API changes, the
   * project is compiled too, since only a compile checks a GraphQL schema of the project instead of
   * the inferred one.
   *
   * @param projects the projects to check
   * @param changes the new content of the changed files
   * @param compile whether to compile the projects as well
   * @return a description of an error that the changes add, if any
   */
  CompletableFuture<Optional<String>> findNewErrors(
      Set<Path> projects, Map<Path, String> changes, boolean compile) {
    try {
      return CompletableFuture.supplyAsync(
          () -> {
            for (var project : projects) {
              for (var fullCompile : compile ? List.of(false, true) : List.of(false)) {
                var overlays = documents.snapshot();
                var changed = new HashMap<>(overlays);
                changed.putAll(changes);
                var newError =
                    NewErrors.between(
                        trialCheck(project, overlays, fullCompile),
                        trialCheck(project, changed, fullCompile));
                if (newError.isPresent()) {
                  return newError;
                }
              }
            }
            return Optional.empty();
          },
          executor);
    } catch (RejectedExecutionException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private CheckResult trialCheck(Path project, Map<Path, String> overlays, boolean fullCompile) {
    var workDir = workRoot.resolve(workDir(project).getFileName() + "-trial");
    return checker.check(
        new CheckRequest(
            project.getParent(),
            packageFiles(project),
            workDir,
            overlays,
            fullCompile,
            fullCompile ? ExecutionGoal.COMPILE : ExecutionGoal.TEST,
            false));
  }

  /** The planned catalog of a project that uses a file, from its last check. */
  Optional<PlannedCatalog> catalogOf(Path file) {
    return projectsOf(file).stream()
        .map(catalogsByProject::get)
        .filter(Objects::nonNull)
        .findFirst();
  }

  /** The build directory of the last check of a project that uses a file. */
  Optional<Path> buildDirOf(Path file) {
    return projectsOf(file).stream()
        .map(project -> workDir(project).resolve(SqrlConstants.BUILD_DIR_NAME))
        .filter(Files::isDirectory)
        .findFirst();
  }

  /** The package files of a project that uses a file. */
  List<Path> packageFilesOf(Path file) {
    return projectsOf(file).stream().findFirst().map(this::packageFiles).orElse(List.of());
  }

  /** The projects that use a file: those that did in their last check, or the enclosing one. */
  Set<Path> projectsOf(Path file) {
    var projects = new LinkedHashSet<>(projectsByFile.getOrDefault(file, Set.of()));
    findProjectRoot(file).ifPresent(projects::add);
    return projects;
  }

  /** The project of a file, which is identified by its first package file. */
  private Optional<Path> findProjectRoot(Path file) {
    var packageFiles =
        ProjectPackages.find(file, settings.packageFiles(), settings.selectedPackageFiles());
    if (packageFiles.isEmpty()) {
      return Optional.empty();
    }
    packageFilesByProject.put(packageFiles.get(0), packageFiles);
    return Optional.of(packageFiles.get(0));
  }

  /**
   * Checks the projects that use a file right away, instead of after the delay for edits, e.g. for
   * a request that needs the positions of the current content.
   *
   * @return completes when the projects have been checked with their current content
   */
  CompletableFuture<Void> checkNow(Path file) {
    return afterChecks(file, true);
  }

  /**
   * Waits for the checks of the projects that use a file, which are scheduled when it changes, e.g.
   * for a request whose answer the editor keeps until the file changes again.
   *
   * @return completes when the projects have been checked with their current content
   */
  CompletableFuture<Void> afterScheduledChecks(Path file) {
    return afterChecks(file, false);
  }

  private synchronized CompletableFuture<Void> afterChecks(Path file, boolean now) {
    if (closed) {
      return CompletableFuture.completedFuture(null);
    }
    var checked = new ArrayList<CompletableFuture<Void>>();
    for (var project : projectsOf(file)) {
      var scheduled = pending.get(project);
      if (!now && (scheduled == null || scheduled.future().isDone())) {
        // The last check is current. Checking again would only repeat it, and a check of a project
        // that fails asks the editor to refresh, which would ask for another check, and so on.
        continue;
      }
      var check = new CompletableFuture<Void>();
      waiting.computeIfAbsent(project, p -> new ArrayList<>()).add(check);
      if (now) {
        schedule(project, false, 0);
      }
      checked.add(check);
    }
    return CompletableFuture.allOf(checked.toArray(CompletableFuture[]::new));
  }

  private void schedule(Path project, boolean fullCompile) {
    schedule(project, fullCompile, settings.checkDelayMillis());
  }

  /** Schedules a check of a project, which replaces a check that was scheduled before. */
  private synchronized void schedule(Path project, boolean fullCompile, long delayMillis) {
    if (closed) {
      return;
    }
    dropped.remove(project);
    var previous = pending.get(project);
    var full = fullCompile;
    if (previous != null && !previous.future().isDone()) {
      // Replaces the previous check, also a running one, which stops at its next statement
      previous.outdated().set(true);
      previous.future().cancel(false);
      full |= previous.fullCompile();
    }
    var runFull = full;
    var outdated = new AtomicBoolean();
    var future =
        executor.schedule(
            () -> run(project, runFull, outdated::get), delayMillis, TimeUnit.MILLISECONDS);
    pending.put(project, new PendingCheck(future, runFull, outdated));
  }

  private void run(Path project, boolean fullCompile, BooleanSupplier outdated) {
    var overlays = documents.snapshot();
    try {
      var request =
          new CheckRequest(
              project.getParent(),
              packageFiles(project),
              workDir(project),
              overlays,
              fullCompile,
              ExecutionGoal.TEST,
              !fullCompile);
      var result = checker.check(request, outdated);
      log.debug(
          "Checked {} in {} ms with {} diagnostics",
          project,
          result.duration().toMillis(),
          result.diagnostics().size());
      if (dropped.contains(project) || outdated.getAsBoolean()) {
        // Dropped, or replaced by a newer check, e.g. after a change since planning finished
        result.catalog().ifPresent(PlannedCatalog::close);
        return;
      }
      keep(project, result);
      if (isStale(overlays, result.sourceFiles())) {
        // Check again, in case the change was not known to belong to this project yet
        schedule(project, fullCompile);
        return;
      }
      publisher.publish(project, packageFiles(project), result);
      checked(project);
    } catch (CancellationException e) {
      // The newer check publishes its result
    } catch (Throwable e) {
      // Also if keeping or publishing the result fails, so that the requests do not wait forever
      log.error("Failed to check {}", project, e);
      try {
        publisher.publishFailure(project, packageFiles(project).get(0), e);
      } finally {
        checked(project);
      }
    }
  }

  /** Keeps the result of a check of a project: the files it uses, its symbols and catalog. */
  private void keep(Path project, CheckResult result) {
    for (var file : result.sourceFiles()) {
      projectsByFile.computeIfAbsent(file, f -> ConcurrentHashMap.newKeySet()).add(project);
    }
    // Keep the previous symbols if nothing could be planned, e.g. while package.json is broken
    if (!result.symbols().symbols().isEmpty() || !result.hasErrors()) {
      symbolsByProject.put(project, result.symbols());
    }
    result.catalog().ifPresent(catalog -> keepCatalog(project, catalog));
  }

  /** Lets the requests that wait for a check of a project continue. */
  private synchronized void checked(Path project) {
    var requests = waiting.remove(project);
    if (requests != null) {
      requests.forEach(request -> request.complete(null));
    }
  }

  /** Keeps the catalog of a project and closes the previous one. */
  private void keepCatalog(Path project, PlannedCatalog catalog) {
    var previous = catalogsByProject.put(project, catalog);
    if (previous != null) {
      previous.close();
    }
    if (closed) {
      catalogsByProject.remove(project, catalog);
      catalog.close();
    }
  }

  /** Whether a source file of the project changed in the editor since the check started. */
  private boolean isStale(Map<Path, String> checkedOverlays, Set<Path> sourceFiles) {
    var current = documents.snapshot();
    return sourceFiles.stream()
        .anyMatch(file -> !Objects.equals(checkedOverlays.get(file), current.get(file)));
  }

  private List<Path> packageFiles(Path project) {
    return packageFilesByProject.getOrDefault(project, List.of(project));
  }

  private Path workDir(Path project) {
    var name = project.getFileName() + "-" + Integer.toHexString(project.toString().hashCode());
    return workRoot.resolve(name);
  }

  /** Stops checking. A running check stops at its next statement, and its result is dropped. */
  @Override
  public void close() {
    synchronized (this) {
      closed = true;
      pending.values().forEach(check -> check.outdated().set(true));
      publisher.close();
      waiting.values().forEach(requests -> requests.forEach(request -> request.complete(null)));
      waiting.clear();
    }
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    catalogsByProject.values().forEach(PlannedCatalog::close);
    catalogsByProject.clear();
    WorkDirectory.delete(workRoot);
  }
}
