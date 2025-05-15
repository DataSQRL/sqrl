package com.datasqrl.planner.hint;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.type.RelDataTypeField;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.planner.parser.SqrlComments;
import com.datasqrl.planner.parser.StatementParserException;

import lombok.Value;

@Value
public class PlannerHints {

  public static final PlannerHints EMPTY = new PlannerHints(List.of());

  List<PlannerHint> hints;

  public Optional<ColumnNamesHint> getQueryByHint() {
    List<PlannerHint> queryby = hints.stream().filter(QueryByHint.class::isInstance).collect(
        Collectors.toList());
    if (queryby.size()>1) {
      throw new StatementParserException(ErrorLabel.GENERIC,
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

  public<H> Optional<H> getHint(Class<H> hintClass) {
    return getHints(hintClass).findFirst();
  }

  public<H> Stream<H> getHints(Class<H> hintClass) {
    return hints.stream().filter(hintClass::isInstance).map(hintClass::cast);
  }

  public void updateColumnNamesHints(Function<String, RelDataTypeField> fieldByIndex) {
    getHints(ColumnNamesHint.class).forEach(hint -> hint.validateAndUpdate(fieldByIndex));
  }


  public static PlannerHints fromHints(SqrlComments comments) {
    List<PlannerHint> hints = comments.getHints().stream()
        .map(PlannerHint::from).collect(Collectors.toUnmodifiableList());
    return new PlannerHints(hints);
  }

}
