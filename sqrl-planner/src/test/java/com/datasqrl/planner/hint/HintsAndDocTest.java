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
package com.datasqrl.planner.hint;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class HintsAndDocTest {

  private static final Map<String, String> COMMENTS =
      Map.of("email", "The email from the comment", "phone", "The phone number");

  @Test
  void givenColumnCommentsWithoutDocString_whenGetDocumentation_thenCommentsDocumentColumns() {
    var documentation = HintsAndDoc.EMPTY.withColumnComments(COMMENTS).getDocumentation();

    assertThat(documentation.docString()).isNull();
    assertThat(documentation.getColumnOpt("email")).contains("The email from the comment");
    assertThat(documentation.getColumnOpt("phone")).contains("The phone number");
  }

  @Test
  void givenDocStringAndColumnComments_whenGetDocumentation_thenDocStringWinsPerColumn() {
    var hintsAndDoc =
        new HintsAndDoc(
                PlannerHints.EMPTY,
                Optional.of("Customers\n# Columns:\n- email: The contact email"))
            .withColumnComments(COMMENTS);

    var documentation = hintsAndDoc.getDocumentation();

    assertThat(documentation.docString()).isEqualTo("Customers");
    assertThat(documentation.getColumnOpt("email")).contains("The contact email");
    assertThat(documentation.getColumnOpt("phone")).contains("The phone number");
  }

  @Test
  void givenColumnComments_whenDroppingHintsOrAddingDoc_thenKeepsComments() {
    var hintsAndDoc = HintsAndDoc.EMPTY.withColumnComments(COMMENTS);

    assertThat(hintsAndDoc.dropHints().columnComments()).isEqualTo(COMMENTS);
    assertThat(hintsAndDoc.updateDocsIfAbsent(Optional.of("Customers")).columnComments())
        .isEqualTo(COMMENTS);
  }
}
