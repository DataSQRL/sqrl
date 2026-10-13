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

import com.datasqrl.lsp.FunctionHelp.Call;
import java.util.List;
import org.junit.jupiter.api.Test;

class FunctionHelpTest {

  /** The calls around the {@code ^} of a text. */
  private static List<Call> callsAt(String text) {
    return FunctionHelp.callsAt(text.replace("^", ""), text.indexOf('^'));
  }

  @Test
  void givenNestedCalls_whenCallsAt_thenInnermostFirstWithTheArgument() {
    assertThat(callsAt("X := SELECT COALESCE(a, SUBSTRING(b, 1, ^"))
        .containsExactly(new Call(List.of("SUBSTRING"), 2), new Call(List.of("COALESCE"), 1));
  }

  @Test
  void givenClosedCallsAndQualifiedNames_whenCallsAt_thenOnlyTheOpenOnes() {
    assertThat(callsAt("X := SELECT * FROM TABLE(ctl.OrdersOf(LEFT(a, 2), ^"))
        .containsExactly(new Call(List.of("ctl", "OrdersOf"), 1), new Call(List.of("TABLE"), 0));
  }

  @Test
  void givenParenthesesWithoutName_whenCallsAt_thenTheyHaveNoName() {
    assertThat(callsAt("X := SELECT ABS((a + ^"))
        .containsExactly(new Call(List.of(), 0), new Call(List.of("ABS"), 0));
  }

  @Test
  void givenCommentsStringsAndOtherStatements_whenCallsAt_thenTheyAreSkipped() {
    assertThat(callsAt("X := SELECT ABS(1;\nY := SELECT LEFT('a,(', /* , ( */ ^"))
        .containsExactly(new Call(List.of("LEFT"), 1));
    assertThat(callsAt("X := SELECT LEFT('a, ^")).isEmpty();
    assertThat(callsAt("X := SELECT LEFT(a -- , ^")).isEmpty();
    // The arguments of hints are not calls
    assertThat(callsAt("/*+ primary_key(a, ^")).isEmpty();
  }
}
