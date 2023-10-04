package com.datasqrl.plan.rules;

import java.util.Optional;

import com.datasqrl.plan.table.TimestampInference;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;

public class LPConverterUtil {

  public static Optional<TimestampInference.Candidate> getTimestampOrderIndex(RelCollation collation, TimestampInference timestamp) {
    if (collation.getFieldCollations().isEmpty()) return Optional.empty();
    RelFieldCollation fieldCol = collation.getFieldCollations().get(0);
    if (fieldCol.direction!= Direction.DESCENDING) return Optional.empty();
    return timestamp.getOptCandidateByIndex(fieldCol.getFieldIndex());
  }

  public static RelCollation getTimestampCollation(TimestampInference.Candidate timestamp) {
    return RelCollations.of(
        new RelFieldCollation(timestamp.getIndex(),
            RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
  }


}
