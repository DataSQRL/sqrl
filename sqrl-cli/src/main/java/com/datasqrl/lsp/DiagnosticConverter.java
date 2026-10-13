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
import com.datasqrl.compile.check.QueryHints;
import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileRange;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.DiagnosticSeverity;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;

/** Converts compiler diagnostics to LSP diagnostics. */
@UtilityClass
class DiagnosticConverter {

  static final String SOURCE = "sqrl";

  /** Calcite's error for an unknown hint in a query */
  private static final Pattern UNKNOWN_QUERY_HINT =
      Pattern.compile("Hint: (\\w+) should be registered in the HintStrategyTable");

  /**
   * Converts a diagnostic. The compiler reports most errors at a single position, which is widened
   * to the word at that position so that editors underline it.
   *
   * @param diagnostic the compiler diagnostic
   * @param text the content of the file the diagnostic refers to, or null if unknown
   */
  static Diagnostic convert(CheckResult.Diagnostic diagnostic, String text) {
    var lines = text == null ? List.<String>of() : text.lines().toList();
    var range =
        diagnostic
            .range()
            .map(r -> toRange(r, lines))
            .orElseGet(() -> new Range(new Position(0, 0), new Position(0, 0)));
    var message = diagnostic.message();
    var unknownHint = UNKNOWN_QUERY_HINT.matcher(message);
    if (unknownHint.find()) {
      // Calcite reports an unknown hint of a query at the start of its statement
      var name = unknownHint.group(1);
      range = text == null ? range : hintInStatement(text, range.getStart(), name).orElse(range);
      message =
          "Unknown hint `%s`. Flink's hints are %s after SELECT, and %s after a table."
              .formatted(
                  name,
                  String.join(", ", QueryHints.QUERY_HINTS),
                  String.join(", ", QueryHints.TABLE_HINTS));
    }
    var result = new Diagnostic(range, message, toSeverity(diagnostic.severity()), SOURCE);
    if (!ErrorLabel.GENERIC.getLabel().equals(diagnostic.code())) {
      result.setCode(diagnostic.code());
    }
    return result;
  }

  /** The name of a hint in a hint comment of the statement at a position, e.g. a misspelled one. */
  private static Optional<Range> hintInStatement(String text, Position start, String name) {
    var script = new ScriptText(text);
    var from = script.offset(start.getLine(), start.getCharacter());
    var end = text.indexOf(';', from);
    var hint =
        Pattern.compile(
                "/\\*\\+[^*]*?\\b(" + Pattern.quote(name) + ")\\b", Pattern.CASE_INSENSITIVE)
            .matcher(text)
            .region(from, end < 0 ? text.length() : end);
    if (!hint.find()) {
      return Optional.empty();
    }
    return Optional.of(
        new Range(
            new Position(script.lineOf(hint.start(1)), script.characterOf(hint.start(1))),
            new Position(script.lineOf(hint.end(1)), script.characterOf(hint.end(1)))));
  }

  static Range toRange(FileRange range, List<String> lines) {
    var start = toPosition(range.fromLine(), range.fromOffset(), lines);
    if (!range.isLocation()) {
      return new Range(start, toPosition(range.toLine(), range.toOffset(), lines));
    }
    var line = lineAt(start.getLine(), lines);
    var startChar = start.getCharacter();
    var endChar = startChar;
    while (endChar < line.length() && isWordChar(line.charAt(endChar))) {
      endChar++;
    }
    if (endChar == startChar) {
      if (startChar >= line.length()) {
        // e.g. a missing `;` after the end of the line: mark the last character
        startChar = Math.max(line.length() - 1, 0);
        endChar = line.length();
      } else {
        endChar = startChar + 1;
      }
    }
    return new Range(
        new Position(start.getLine(), startChar), new Position(start.getLine(), endChar));
  }

  private static Position toPosition(int line, int offset, List<String> lines) {
    var lineIndex = Math.max(Math.min(line - 1, lines.size() - 1), 0);
    var character = Math.max(Math.min(offset - 1, lineAt(lineIndex, lines).length()), 0);
    return new Position(lineIndex, character);
  }

  private static String lineAt(int index, List<String> lines) {
    return index < lines.size() ? lines.get(index) : "";
  }

  private static boolean isWordChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '`';
  }

  private static DiagnosticSeverity toSeverity(com.datasqrl.error.ErrorMessage.Severity severity) {
    return switch (severity) {
      case FATAL -> DiagnosticSeverity.Error;
      case WARN -> DiagnosticSeverity.Warning;
      case NOTICE -> DiagnosticSeverity.Information;
    };
  }
}
