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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.planner.tags.InvalidTag.Reason;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TableTagsTest {

  static final TagDefinitions DEFS =
      TagDefinitions.of(
          List.of(
              new TagDefinition(
                  "classification", List.of("unclassified", "secret", "top_secret"), List.of()),
              new TagDefinition("pii", List.of("masked", "raw"), List.of()),
              new TagDefinition("gdpr", List.of(), List.of())));

  @Test
  void givenTags_whenBuild_thenNormalizesAndResolvesOffsets() {
    var tags =
        TableTags.builder(DEFS).add("PII", "RAW").add("gdpr", null).add("Gdpr", "true").build();

    assertThat(tags.get("pii")).hasValue(1);
    assertThat(tags.get("gdpr")).hasValue(TagDefinition.BOOLEAN_OFFSET);
    assertThat(tags.names()).containsExactlyInAnyOrder("pii", "gdpr");
    assertThat(tags.hasInvalidTags()).isFalse();
  }

  @Test
  void givenDuplicateTags_whenBuild_thenKeepsMaximum() {
    var tags =
        TableTags.builder(DEFS)
            .add("classification", "top_secret")
            .add("classification", "secret")
            .build();

    assertThat(tags.valueOf("classification")).isEqualTo("top_secret");
  }

  @Test
  void givenInvalidTags_whenBuild_thenRecordsInvalidTags() {
    var tags =
        TableTags.builder(DEFS)
            .add("unknown", null)
            .add("pii", "nope")
            .add("pii", null)
            .add("gdpr", "false")
            .build();

    assertThat(tags.names()).isEmpty();
    assertThat(tags.getInvalidTags())
        .extracting(InvalidTag::text, InvalidTag::reason)
        .containsExactly(
            tuple("@unknown", Reason.UNDEFINED_TAG),
            tuple("@pii=nope", Reason.INVALID_VALUE),
            tuple("@pii", Reason.MISSING_VALUE),
            tuple("@gdpr=false", Reason.NON_TRUE_BOOLEAN));
    assertThat(tags.getInvalidTags().get(1).message(DEFS))
        .isEqualTo("Invalid value in tag [@pii=nope]: allowed values are [masked, raw]");
    assertThat(tags.withoutInvalidTags()).isSameAs(TableTags.EMPTY);
  }

  @Test
  void givenDisabledDefinitions_whenBuild_thenEmpty() {
    var tags = TableTags.builder(TagDefinitions.DISABLED).add("pii", "raw").build();

    assertThat(tags).isSameAs(TableTags.EMPTY);
  }

  @Test
  void givenTwoTagSets_whenMerge_thenUnionWithMaximum() {
    var a = TableTags.builder(DEFS).add("classification", "secret").add("pii", "masked").build();
    var b = TableTags.builder(DEFS).add("classification", "unclassified").add("gdpr", null).build();

    var merged = a.merge(b);

    assertThat(merged.toValueMap())
        .containsExactly(
            entry("classification", "secret"), entry("gdpr", "true"), entry("pii", "masked"));
    assertThat(merged.toString()).isEqualTo("classification=secret, gdpr=true, pii=masked");
    assertThat(a.merge(TableTags.EMPTY)).isSameAs(a);
    assertThat(TableTags.EMPTY.merge(b)).isSameAs(b);
  }

  @Test
  void givenDeclassifiedTags_whenDeclassify_thenLowersOrRemoves() {
    var inherited =
        TableTags.builder(DEFS)
            .add("classification", "top_secret")
            .add("pii", "raw")
            .add("gdpr", null)
            .build();
    var declared = TableTags.builder(DEFS).add("classification", "unclassified").build();

    var result = inherited.declassify(Set.of("classification", "PII"), declared);

    assertThat(result.toString()).isEqualTo("classification=unclassified, gdpr=true");
  }

  @Test
  void givenDifferentDefinitions_whenMerge_thenRejected() {
    var otherDefs = TagDefinitions.of(List.of(new TagDefinition("pii", List.of(), List.of())));
    var a = TableTags.builder(DEFS).add("pii", "raw").build();
    var b = TableTags.builder(otherDefs).add("pii", null).build();

    assertThatThrownBy(() -> a.merge(b)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void givenOffsets_whenAddOffset_thenBuildsTagSet() {
    var tags = TableTags.builder(DEFS).addOffset("pii", 0).addOffset("pii", 1).build();

    assertThat(tags.valueOf("pii")).isEqualTo("raw");
    assertThatThrownBy(() -> TableTags.builder(DEFS).addOffset("pii", 5))
        .isInstanceOf(IndexOutOfBoundsException.class);
  }
}
