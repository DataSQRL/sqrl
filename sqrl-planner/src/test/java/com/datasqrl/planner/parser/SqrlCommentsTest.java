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
package com.datasqrl.planner.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.hint.DocStringParser;
import org.junit.jupiter.api.Test;

class SqrlCommentsTest {

  @Test
  void givenDocCommentWithLineDecoration_whenDocString_thenRemovesDecoration() {
    var comments =
        parse(
            """
            /** Customer profiles @pii=raw
             * # Columns:
             * - address: The address
             *   - street: The street
             *
             * More about **customers**
             */""");

    assertThat(comments.docString())
        .contains(
            "Customer profiles @pii=raw\n# Columns:\n- address: The address\n  - street: The"
                + " street\n\nMore about **customers**");
  }

  @Test
  void givenColumnListWithDecorationAsBullets_whenParsingDocString_thenDocumentsColumns() {
    var comments =
        parse(
            """
            /** # Columns:
             * id: The customer id
             * email: The contact email
             */""");

    var documentation = DocStringParser.parse(comments.docString().orElseThrow());

    assertThat(documentation.docString()).isNull();
    assertThat(documentation.columnDocs())
        .containsExactly(
            entry(NamePath.of("id"), "The customer id"),
            entry(NamePath.of("email"), "The contact email"));
  }

  @Test
  void givenTwoDocComments_whenDocString_thenRemovesDecorationOfEach() {
    var comments =
        parse(
            """
            /** Customer profiles */
            /**
             * # Columns:
             * - id: The id
             */""");

    assertThat(comments.docString()).contains("Customer profiles\n# Columns:\n- id: The id");
  }

  @Test
  void givenLineWithoutDecoration_whenDocString_thenKeepsTheStars() {
    var comments =
        parse(
            """
            /** Orders
            Notes:
             * late orders are flagged
             */""");

    assertThat(comments.docString()).contains("Orders\nNotes:\n * late orders are flagged");
  }

  @Test
  void givenStarBulletsThatStartTheLines_whenDocString_thenKeepsThem() {
    var comments =
        parse(
            """
            /** Orders. Notes:
            * late orders are flagged
            * totals are in cents
            */""");

    assertThat(comments.docString())
        .contains("Orders. Notes:\n* late orders are flagged\n* totals are in cents");
  }

  @Test
  void givenNoDocComment_whenDocString_thenEmpty() {
    assertThat(parse("/*+no_query */").docString()).isEmpty();
  }

  private static SqrlComments parse(String comments) {
    return SqrlComments.parse(new ParsedObject<>(comments, FileLocation.START));
  }
}
