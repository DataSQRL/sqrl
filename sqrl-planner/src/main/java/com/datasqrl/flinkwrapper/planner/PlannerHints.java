package com.datasqrl.flinkwrapper.planner;

import com.datasqrl.flinkwrapper.parser.SqrlComments;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class PlannerHints {

  public static final PlannerHints EMPTY = new PlannerHints(List.of());

  List<PlannerHint> hints;

  public boolean isTest() {
    return hints.stream().anyMatch(TestHint.class::isInstance);
  }

  public boolean isQuery() {
    return hints.stream().anyMatch(QueryHint.class::isInstance);
  }

  public boolean isQuerySink() {
    return isTest() || isQuery();
  }

  public static PlannerHints fromHints(SqrlComments comments) {
    List<PlannerHint> hints = comments.getHints().stream()
        .map(PlannerHint::from).collect(Collectors.toUnmodifiableList());
    return new PlannerHints(hints);
  }

}
