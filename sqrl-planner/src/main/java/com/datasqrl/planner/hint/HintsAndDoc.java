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

import com.datasqrl.planner.tags.TableTags;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.planner.util.Documented;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.Optional;

/**
 * The hints and doc string of a statement.
 *
 * @param hints the planner hints
 * @param doc the raw doc string
 * @param tagDefinitions the tag definitions used to extract tags from the doc string
 * @param columnTags tags declared on individual columns (e.g. column comments in CREATE TABLE),
 *     which count toward the table's declared tags
 */
public record HintsAndDoc(
    PlannerHints hints, Optional<String> doc, TagDefinitions tagDefinitions, TableTags columnTags) {

  public static final HintsAndDoc EMPTY = new HintsAndDoc(PlannerHints.EMPTY, Optional.empty());

  public HintsAndDoc(PlannerHints hints, Optional<String> doc) {
    this(hints, doc, TagDefinitions.DISABLED, TableTags.EMPTY);
  }

  public HintsAndDoc(PlannerHints hints, Optional<String> doc, TagDefinitions tagDefinitions) {
    this(hints, doc, tagDefinitions, TableTags.EMPTY);
  }

  public HintsAndDoc dropHints() {
    return new HintsAndDoc(PlannerHints.EMPTY, doc, tagDefinitions, columnTags);
  }

  public HintsAndDoc updateDocsIfAbsent(Optional<String> documentation) {
    return this.doc.isPresent()
        ? this
        : new HintsAndDoc(hints, documentation, tagDefinitions, columnTags);
  }

  public HintsAndDoc withColumnTags(TableTags columnTags) {
    return new HintsAndDoc(hints, doc, tagDefinitions, columnTags);
  }

  /**
   * Extracts Column and Argument documentation from markdown-style syntax. Any remaining content is
   * considered the table or function docString. Tags are extracted from the doc string and combined
   * with the {@link #columnTags()}.
   *
   * @return documentation with parsed column/argument docs, the remaining description and tags
   */
  public Documentation getDocumentation() {
    var documentation =
        doc.map(d -> DocStringParser.parse(d, tagDefinitions)).orElse(Documented.EMPTY);
    if (columnTags.isEmpty()) {
      return documentation;
    }
    return documentation.withTags(documentation.tags().merge(columnTags));
  }
}
