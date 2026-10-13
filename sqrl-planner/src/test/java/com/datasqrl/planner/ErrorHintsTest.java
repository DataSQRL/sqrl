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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorLabel;
import java.util.List;
import org.junit.jupiter.api.Test;

class ErrorHintsTest {

  @Test
  void givenTypo_whenClosestMatch_thenSuggestsCandidate() {
    assertThat(ErrorHints.closestMatch("Mesages", List.of("Messages", "HelloWorld")))
        .contains("Messages");
    assertThat(ErrorHints.closestMatch("helloworld", List.of("Messages", "HelloWorld")))
        .contains("HelloWorld");
  }

  @Test
  void givenUnrelatedName_whenClosestMatch_thenSuggestsNothing() {
    assertThat(ErrorHints.closestMatch("Customers", List.of("Messages", "HelloWorld"))).isEmpty();
  }

  @Test
  void givenStrings_whenDistance_thenReturnsCaseInsensitiveLevenshteinDistance() {
    assertThat(ErrorHints.distance("Mesages", "Messages")).isEqualTo(1);
    assertThat(ErrorHints.distance("ABC", "abc")).isZero();
    assertThat(ErrorHints.distance("", "abc")).isEqualTo(3);
  }

  @Test
  void givenParserErrorInMergedStatements_whenExplain_thenHintsAtMissingSemicolon() {
    var statement = "A := SELECT * FROM Messages\n/*+ test */\nB := SELECT COUNT(*) FROM A;";

    var hint = ErrorHints.explain("Encountered \"/*+\" .", statement, 2, null);

    assertThat(hint.label()).isEqualTo(ErrorLabel.GENERIC);
    assertThat(hint.message()).contains("';' missing at the end of the previous statement");
  }

  @Test
  void givenParserErrorInSingleStatement_whenExplain_thenOmitsExpectedTokens() {
    var hint =
        ErrorHints.explain(
            "Encountered \"FORM\" at line 1.\nWas expecting one of:\n    \"FROM\" ...",
            "A := SELECT * FORM Messages;",
            1,
            null);

    assertThat(hint.message()).isEqualTo("Encountered \"FORM\" at line 1.");
  }

  @Test
  void givenParserErrorOnFirstLine_whenExplain_thenOmitsSemicolonHint() {
    var statement = "A := SELECT 'unterminated FROM Messages;\n\nB := SELECT * FROM Messages;";

    var hint = ErrorHints.explain("Encountered \"\\'\" .", statement, 1, null);

    assertThat(hint.message()).doesNotContain("';' missing");
  }
}
