package com.datasqrl.plan.rules;

import java.util.Optional;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;

import com.datasqrl.plan.table.Timestamps;

public class LPConverterUtil {

  public static Optional<Integer> getTimestampOrderIndex(RelCollation collation, Timestamps timestamp) {
    if (collation.getFieldCollations().isEmpty()) {
		return Optional.empty();
	}
    var fieldCol = collation.getFieldCollations().getFirst();
    if (timestamp.isCandidate(fieldCol.getFieldIndex())) {
		return Optional.of(fieldCol.getFieldIndex());
	}
    return Optional.empty();
  }

  public static RelCollation getTimestampCollation(int timestampIndex) {
    return RelCollations.of(
        new RelFieldCollation(timestampIndex,
            RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
  }


}
