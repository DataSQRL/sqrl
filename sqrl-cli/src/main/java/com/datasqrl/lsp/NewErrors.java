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
import com.datasqrl.error.ErrorMessage.Severity;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** The errors that a change adds to a project, from trial checks before and after the change. */
final class NewErrors {

  private NewErrors() {}

  /**
   * Describes the first error after a change that was not there before, if the change adds errors.
   */
  static Optional<String> between(CheckResult before, CheckResult after) {
    var errorsBefore = errors(before);
    var errorsAfter = errors(after);
    if (errorsAfter.size() <= errorsBefore.size()) {
      return Optional.empty();
    }
    var previous = new ArrayList<>(errorsBefore.stream().map(NewErrors::key).toList());
    var error =
        errorsAfter.stream()
            .filter(e -> !previous.remove(key(e)))
            .findFirst()
            .orElse(errorsAfter.get(0));
    return Optional.of(describe(error));
  }

  private static List<CheckResult.Diagnostic> errors(CheckResult result) {
    return result.diagnostics().stream().filter(d -> d.severity() == Severity.FATAL).toList();
  }

  private static String key(CheckResult.Diagnostic diagnostic) {
    return diagnostic.file().map(Path::toString).orElse("") + ":" + diagnostic.message();
  }

  private static String describe(CheckResult.Diagnostic error) {
    var where =
        error
            .file()
            .map(
                file ->
                    " (`%s`%s)"
                        .formatted(
                            file.getFileName(),
                            error.range().map(r -> " line " + r.fromLine()).orElse("")))
            .orElse("");
    return error.message() + where;
  }
}
