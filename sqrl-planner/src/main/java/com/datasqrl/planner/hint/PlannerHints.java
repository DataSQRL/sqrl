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

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.planner.parser.SqrlComments;
import com.datasqrl.planner.parser.StatementParserException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataTypeField;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class PlannerHints {

  public static final PlannerHints EMPTY = new PlannerHints(List.of());

  private final List<PlannerHint> hints;

  public static PlannerHints from(
      SqrlComments comments, Optional<PlannerHints> inheritedHints, ErrorCollector errors) {
    var hints = new ArrayList<PlannerHint>();
    inheritedHints.ifPresent(inherited -> hints.addAll(inherited.hints));

    comments.hints().stream()
        .map(c -> PlannerHint.from(c, errors))
        .flatMap(Optional::stream)
        .forEach(hints::add);

    return new PlannerHints(hints);
  }

  public Optional<ColumnNamesHint> getQueryByHint() {
    var queryBy = hints.stream().filter(QueryByHint.class::isInstance).toList();
    if (queryBy.size() > 1) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          queryBy.get(1).getSource().getFileLocation(),
          "Cannot have more than one `query_by_*` or `no_query` hint per table");
    }

    return queryBy.stream().map(ColumnNamesHint.class::cast).findFirst();
  }

  public boolean isEmpty() {
    return hints.isEmpty();
  }

  public boolean isTest() {
    return getHint(TestHint.class).isPresent();
  }

  public boolean isNoRowsTest() {
    return getHint(TestHint.class).map(TestHint::isNoRows).orElse(false);
  }

  public boolean isWorkload() {
    return getHint(WorkloadHint.class).isPresent();
  }

  public <H> Optional<H> getHint(Class<H> hintClass) {
    return getHints(hintClass).findFirst();
  }

  public <H> Stream<H> getHints(Class<H> hintClass) {
    return hints.stream().filter(hintClass::isInstance).map(hintClass::cast);
  }

  public void updateColumnNamesHints(Function<String, RelDataTypeField> fieldByIndex) {
    getHints(ColumnNamesHint.class).forEach(hint -> hint.validateAndUpdate(fieldByIndex));
  }
}
