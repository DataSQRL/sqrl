package com.datasqrl.plan.calcite.rules;

import com.datasqrl.plan.calcite.table.TimestampHolder;
import com.datasqrl.plan.calcite.table.TimestampHolder.Derived;
import java.util.Optional;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;

public class LPConverterUtil {

  public static Optional<Derived.Candidate> getTimestampOrderIndex(RelCollation collation, TimestampHolder.Derived timestamp) {
    if (collation.getFieldCollations().isEmpty()) return Optional.empty();
    RelFieldCollation fieldCol = collation.getFieldCollations().get(0);
    if (fieldCol.direction!= Direction.DESCENDING) return Optional.empty();
    return timestamp.getOptCandidateByIndex(fieldCol.getFieldIndex());
  }

  public static RelCollation getTimestampCollation(TimestampHolder.Candidate timestamp) {
    return RelCollations.of(
        new RelFieldCollation(timestamp.getIndex(),
            RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
  }


}
