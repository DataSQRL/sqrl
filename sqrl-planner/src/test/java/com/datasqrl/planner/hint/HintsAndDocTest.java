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

import com.datasqrl.planner.tags.TagDefinition;
import com.datasqrl.planner.tags.TagDefinitions;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class HintsAndDocTest {

  private static final TagDefinitions TAG_DEFS =
      TagDefinitions.of(
          List.of(
              new TagDefinition("pii", List.of("masked", "raw"), List.of()),
              new TagDefinition("gdpr", List.of(), List.of())));

  @Test
  void givenDocAndColumnTags_whenGetDocumentation_thenTagsAreCombined() {
    var columnTags = DocStringParser.parseTags("@pii=raw", TAG_DEFS);
    var hintsAndDoc =
        new HintsAndDoc(PlannerHints.EMPTY, Optional.of("Table @gdpr @pii=masked"), TAG_DEFS)
            .withColumnTags(columnTags);

    var documentation = hintsAndDoc.getDocumentation();

    assertThat(documentation.tags().toString()).isEqualTo("gdpr=true, pii=raw");
    assertThat(documentation.docString()).isEqualTo("Table @gdpr @pii=masked");
  }

  @Test
  void givenColumnTagsOnly_whenGetDocumentation_thenTagsFromColumns() {
    var hintsAndDoc =
        new HintsAndDoc(PlannerHints.EMPTY, Optional.empty(), TAG_DEFS)
            .withColumnTags(DocStringParser.parseTags("@gdpr", TAG_DEFS));

    var documentation = hintsAndDoc.getDocumentation();

    assertThat(documentation.tags().toString()).isEqualTo("gdpr=true");
    assertThat(documentation.hasDocString()).isFalse();
    assertThat(hintsAndDoc.updateDocsIfAbsent(Optional.of("doc")).columnTags())
        .isEqualTo(hintsAndDoc.columnTags());
  }
}
