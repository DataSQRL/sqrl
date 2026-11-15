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

import com.datasqrl.planner.util.Documented;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.Map;
import java.util.Optional;

/**
 * The hints and doc string of a statement.
 *
 * @param hints the hints
 * @param doc the doc string
 * @param columnComments the comments of the columns of a created table, by column name, which
 *     document the columns that the doc string doesn't
 */
public record HintsAndDoc(
    PlannerHints hints, Optional<String> doc, Map<String, String> columnComments) {

  public static final HintsAndDoc EMPTY = new HintsAndDoc(PlannerHints.EMPTY, Optional.empty());

  public HintsAndDoc(PlannerHints hints, Optional<String> doc) {
    this(hints, doc, Map.of());
  }

  public HintsAndDoc dropHints() {
    return new HintsAndDoc(PlannerHints.EMPTY, doc, columnComments);
  }

  public HintsAndDoc updateDocsIfAbsent(Optional<String> documentation) {
    return this.doc.isPresent() ? this : new HintsAndDoc(hints, documentation, columnComments);
  }

  public HintsAndDoc withColumnComments(Map<String, String> comments) {
    return new HintsAndDoc(hints, doc, comments);
  }

  /**
   * Extracts Column and Argument documentation from markdown-style syntax. Any remaining content is
   * considered the table or function docString. A column that the doc string doesn't document gets
   * its comment as its documentation.
   *
   * @return documentation with parsed column/argument docs and the remaining description
   */
  public Documentation getDocumentation() {
    return doc.map(DocStringParser::parse)
        .orElse(Documented.EMPTY)
        .withColumnDocsIfAbsent(columnComments);
  }
}
