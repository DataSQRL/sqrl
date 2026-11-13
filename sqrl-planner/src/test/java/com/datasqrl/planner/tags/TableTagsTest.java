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
package com.datasqrl.planner.tags;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TableTagsTest {

  private static final TagDefinition CLASSIFICATION =
      TagDefinition.of("classification", List.of("unclassified", "secret"), List.of());
  private static final TagDefinition PII =
      TagDefinition.of("pii", List.of("masked", "raw"), List.of());
  private static final TagDefinition GDPR = TagDefinition.of("gdpr", List.of(), List.of());

  @Test
  void givenDuplicateTags_whenOf_thenKeepsHighestValue() {
    var tags = TableTags.of(List.of(new TagValue(PII, 1), new TagValue(PII, 0)));

    assertThat(tags).hasToString("@pii=raw");
  }

  @Test
  void givenTwoTagSets_whenMerge_thenUnionWithHighestValue() {
    var first = TableTags.of(List.of(new TagValue(CLASSIFICATION, 1), new TagValue(PII, 0)));
    var second = TableTags.of(List.of(new TagValue(PII, 1), new TagValue(GDPR, 0)));

    assertThat(first.merge(second)).hasToString("@classification=secret @gdpr @pii=raw");
    assertThat(first.merge(TableTags.EMPTY)).isEqualTo(first);
  }

  @Test
  void givenTags_whenWithout_thenRemovesTheGivenTags() {
    var tags = TableTags.of(List.of(new TagValue(CLASSIFICATION, 1), new TagValue(PII, 1)));

    assertThat(tags.without(Set.of("pii", "gdpr"))).hasToString("@classification=secret");
  }

  @Test
  void givenTags_whenToValueMap_thenMapsSortedNamesToValues() {
    var tags = TableTags.of(List.of(new TagValue(PII, 0), new TagValue(GDPR, 0)));

    assertThat(tags.toValueMap()).containsExactly(entry("gdpr", "true"), entry("pii", "masked"));
  }
}
