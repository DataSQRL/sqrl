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
import java.util.Optional;

public record HintsAndDoc(PlannerHints hints, Optional<String> doc) {

  public static final HintsAndDoc EMPTY = new HintsAndDoc(PlannerHints.EMPTY, Optional.empty());

  public HintsAndDoc dropHints() {
    return new HintsAndDoc(PlannerHints.EMPTY, doc);
  }

  public HintsAndDoc updateDocsIfAbsent(Optional<String> documentation) {
    return this.doc.isPresent() ? this : new HintsAndDoc(hints, documentation);
  }

  /**
   * Extracts Column and Argument documentation from markdown-style syntax. Any remaining content is
   * considered the table or function docString.
   *
   * @return Documentation with parsed column/argument docs and remaining description
   */
  public Documentation getDocumentation() {
    if (doc.isPresent()) {
      return DocStringParser.parse(doc.get());
    }
    return Documented.EMPTY;
  }
}
