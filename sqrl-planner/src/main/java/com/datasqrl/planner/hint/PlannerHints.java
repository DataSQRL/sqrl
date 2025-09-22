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

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.planner.parser.SqrlComments;
import com.datasqrl.planner.parser.StatementParserException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataTypeField;

@Value
public class PlannerHints {

  public static final PlannerHints EMPTY = new PlannerHints(List.of());

  List<PlannerHint> hints;

  public Optional<ColumnNamesHint> getQueryByHint() {
    List<PlannerHint> queryby =
        hints.stream().filter(QueryByHint.class::isInstance).collect(Collectors.toList());
    if (queryby.size() > 1) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          queryby.get(1).getSource().getFileLocation(),
          "Cannot have more than one `query_by_*` or `no_query` hint per table");
    }
    return queryby.stream().map(ColumnNamesHint.class::cast).findFirst();
  }

  public boolean isTest() {
    return hints.stream().anyMatch(TestHint.class::isInstance);
  }

  public boolean isWorkload() {
    return hints.stream().anyMatch(WorkloadHint.class::isInstance);
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

  public static PlannerHints fromHints(SqrlComments comments) {
    List<PlannerHint> hints =
        comments.hints().stream().map(PlannerHint::from).collect(Collectors.toUnmodifiableList());
    return new PlannerHints(hints);
  }
}
