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

import com.datasqrl.error.ErrorLocation.FileRange;
import com.datasqrl.error.ErrorMessage.Severity;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The outcome of a {@link ScriptChecker} run.
 *
 * @param diagnostics the errors, warnings and notices of the compiler
 * @param symbols the tables and functions that were planned, also when planning failed for some,
 *     and the names in queries that refer to them
 * @param sourceFiles the source files that the project uses, including those of included packages:
 *     the scripts that were planned, and all other files
 * @param catalog the planned catalog, if the request asked to keep it; the caller must close it
 * @param duration how long the check took
 */
public record CheckResult(
    List<Diagnostic> diagnostics,
    SymbolIndex symbols,
    Set<Path> sourceFiles,
    Optional<PlannedCatalog> catalog,
    Duration duration) {

  public boolean hasErrors() {
    return diagnostics.stream().anyMatch(d -> d.severity() == Severity.FATAL);
  }

  /**
   * A compiler message.
   *
   * @param severity the severity
   * @param message the message
   * @param file the source file the message refers to, if known
   * @param range the 1-based position in that file, if known
   * @param code the error code, e.g. {@code GENERIC_ERROR}
   */
  public record Diagnostic(
      Severity severity,
      String message,
      Optional<Path> file,
      Optional<FileRange> range,
      String code) {}
}
