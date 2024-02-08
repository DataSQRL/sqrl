package com.datasqrl.plan.rules;

import com.datasqrl.plan.table.Timestamps;
import java.util.Optional;

import com.datasqrl.plan.table.TimestampInference;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;

public class LPConverterUtil {

  public static Optional<Integer> getTimestampOrderIndex(RelCollation collation, Timestamps timestamp) {
    if (collation.getFieldCollations().isEmpty()) return Optional.empty();
    RelFieldCollation fieldCol = collation.getFieldCollations().get(0);
    if (fieldCol.direction!= Direction.DESCENDING) return Optional.empty();
    return Optional.of(fieldCol.getFieldIndex());
  }

  public static RelCollation getTimestampCollation(int timestampIndex) {
    return RelCollations.of(
        new RelFieldCollation(timestampIndex,
            RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
  }


}
