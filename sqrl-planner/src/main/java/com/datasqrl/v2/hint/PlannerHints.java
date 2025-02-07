package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.SqrlComments;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

@Value
public class PlannerHints {

  public static final PlannerHints EMPTY = new PlannerHints(List.of());

  List<PlannerHint> hints;

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


  public static PlannerHints fromHints(SqrlComments comments) {
    List<PlannerHint> hints = comments.getHints().stream()
        .map(PlannerHint::from).collect(Collectors.toUnmodifiableList());
    return new PlannerHints(hints);
  }

}
