package com.datasqrl.planner.hint;

import java.util.Optional;

public record HintsAndDocs(PlannerHints hints, Optional<String> documentation) {

  public static final HintsAndDocs EMPTY = new HintsAndDocs(PlannerHints.EMPTY, Optional.empty());

  public String getDocumentation() {
    return documentation.orElse("");
  }

  public HintsAndDocs dropHints() {
    return new HintsAndDocs(PlannerHints.EMPTY, documentation);
  }

  public HintsAndDocs updateDocsIfAbsent(Optional<String> documentation) {
    if (documentation.isPresent()) return this;
    return new HintsAndDocs(hints, documentation);
  }
}
