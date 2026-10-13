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

import static com.datasqrl.lsp.TestLanguageServers.TIMEOUT_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.compile.check.CheckRequest;
import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.compile.check.ScriptChecker;
import com.datasqrl.compile.check.SymbolIndex;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Schedules the checks of projects when their files change. */
class ProjectChecksTest {

  @TempDir Path tempDir;

  private CheckTestProject project;
  private final RunningChecker checker = new RunningChecker();

  @BeforeEach
  void setUp() throws Exception {
    project = CheckTestProject.create(tempDir);
  }

  @Test
  void givenRunningCheck_whenTheScriptChangesAgain_thenTheNewerCheckStopsAndReplacesIt()
      throws Exception {
    try (var checks = new ProjectChecks(new OpenDocuments(), tempDir.resolve("work"), checker)) {
      checks.fileChanged(project.mainScript(), false);
      var running = checker.started();
      var checked = checks.afterScheduledChecks(project.mainScript());

      checks.fileChanged(project.mainScript(), false);

      assertThat(running.outdated().getAsBoolean()).isTrue();
      var newer = checker.started();
      assertThat(newer.outdated().getAsBoolean()).isFalse();
      // The requests that waited for the stopped check get the result of the newer one
      checked.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Test
  void givenRunningFullCompile_whenTheScriptChanges_thenTheNewerCheckCompiles() throws Exception {
    try (var checks = new ProjectChecks(new OpenDocuments(), tempDir.resolve("work"), checker)) {
      checks.fileChanged(project.mainScript(), true);
      checker.started();

      checks.fileChanged(project.mainScript(), false);

      assertThat(checker.started().request().fullCompile()).isTrue();
    }
  }

  @Test
  void givenResultThatCannotBeKept_whenChecked_thenTheRequestsThatWaitForItContinue()
      throws Exception {
    // A result without symbols, which fails after the check, when the symbols are kept
    var failing =
        new ScriptChecker() {
          @Override
          public CheckResult check(CheckRequest request, BooleanSupplier outdated) {
            return new CheckResult(List.of(), null, Set.of(), Optional.empty(), Duration.ZERO);
          }
        };
    try (var checks = new ProjectChecks(new OpenDocuments(), tempDir.resolve("work"), failing)) {
      checks.fileChanged(project.mainScript(), false);

      checks.afterScheduledChecks(project.mainScript()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Test
  void givenLastOpenFileOfProject_whenClosed_thenTheProjectIsForgotten() throws Exception {
    var documents = new OpenDocuments();
    var instant =
        new ScriptChecker() {
          @Override
          public CheckResult check(CheckRequest request, BooleanSupplier outdated) {
            return new CheckResult(
                List.of(), SymbolIndex.EMPTY, Set.of(), Optional.empty(), Duration.ZERO);
          }
        };
    try (var checks = new ProjectChecks(documents, tempDir.resolve("work"), instant)) {
      var main = project.mainScript();
      documents.open(main, main.toUri().toString(), CheckTestProject.MAIN_SCRIPT);
      checks.fileChanged(main, false);
      checks.afterScheduledChecks(main).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertThat(checks.symbolsByProjectOf(main)).isNotEmpty();

      documents.close(main);
      checks.fileClosed(main);

      // Its catalog, symbols and diagnostics are released until a file of it is opened again
      assertThat(checks.symbolsByProjectOf(main)).isEmpty();
    }
  }

  /** A check that the checker started. */
  private record Started(CheckRequest request, BooleanSupplier outdated) {}

  /**
   * Runs the first check until a newer one replaces it, as a long check of a large project would,
   * and the others at once.
   */
  private static final class RunningChecker extends ScriptChecker {

    private final BlockingQueue<Started> started = new LinkedBlockingQueue<>();
    private final AtomicBoolean first = new AtomicBoolean(true);

    Started started() throws InterruptedException {
      var check = started.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertThat(check).as("a started check").isNotNull();
      return check;
    }

    @Override
    public CheckResult check(CheckRequest request, BooleanSupplier outdated) {
      started.add(new Started(request, outdated));
      if (first.getAndSet(false)) {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!outdated.getAsBoolean() && System.nanoTime() < deadline) {
          Thread.onSpinWait();
        }
        // As the planner does at the next statement
        throw new CancellationException("A newer check replaces this one");
      }
      return new CheckResult(
          List.of(), SymbolIndex.EMPTY, Set.of(), Optional.empty(), Duration.ZERO);
    }
  }
}
