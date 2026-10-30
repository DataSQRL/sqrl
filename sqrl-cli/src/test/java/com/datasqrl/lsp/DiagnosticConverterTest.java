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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.error.ErrorLocation.FileRange;
import com.datasqrl.error.ErrorMessage.Severity;
import java.util.List;
import java.util.Optional;
import org.eclipse.lsp4j.DiagnosticSeverity;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.junit.jupiter.api.Test;

class DiagnosticConverterTest {

  private static final List<String> LINES =
      List.of("Orders := SELECT * FROM Stream;", "Totals := SELECT `my col` FROM Orders");

  @Test
  void givenPointLocation_whenToRange_thenWidensToWord() {
    var range = DiagnosticConverter.toRange(new FileRange(1, 25, 1, 25), LINES);

    assertThat(range).isEqualTo(range(0, 24, 0, 30));
  }

  @Test
  void givenPointAfterEndOfLine_whenToRange_thenMarksLastCharacter() {
    var range = DiagnosticConverter.toRange(new FileRange(2, 38, 2, 38), LINES);

    assertThat(range).isEqualTo(range(1, 36, 1, 37));
  }

  @Test
  void givenPointOnSymbol_whenToRange_thenMarksOneCharacter() {
    var range = DiagnosticConverter.toRange(new FileRange(1, 8, 1, 8), LINES);

    assertThat(range).isEqualTo(range(0, 7, 0, 8));
  }

  @Test
  void givenLineBeyondFile_whenToRange_thenClampsToLastLine() {
    var range = DiagnosticConverter.toRange(new FileRange(9, 1, 9, 1), LINES);

    assertThat(range.getStart().getLine()).isEqualTo(1);
  }

  @Test
  void givenRange_whenToRange_thenKeepsIt() {
    var range = DiagnosticConverter.toRange(new FileRange(1, 1, 2, 7), LINES);

    assertThat(range).isEqualTo(range(0, 0, 1, 6));
  }

  @Test
  void givenWarningWithCode_whenConvert_thenMapsSeverityAndCode() {
    var diagnostic =
        new CheckResult.Diagnostic(
            Severity.WARN,
            "The rowtime column is nullable",
            Optional.empty(),
            Optional.of(new FileRange(1, 1, 1, 1)),
            "ROWTIME_IS_NULLABLE");

    var converted = DiagnosticConverter.convert(diagnostic, String.join("\n", LINES));

    assertThat(converted.getSeverity()).isEqualTo(DiagnosticSeverity.Warning);
    assertThat(converted.getCode().getLeft()).isEqualTo("ROWTIME_IS_NULLABLE");
    assertThat(converted.getSource()).isEqualTo("sqrl");
    assertThat(converted.getRange()).isEqualTo(range(0, 0, 0, 6));
  }

  private static Range range(int startLine, int startChar, int endLine, int endChar) {
    return new Range(new Position(startLine, startChar), new Position(endLine, endChar));
  }
}
