package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public class TimestampUtil {
  private static final Map<Name, Integer> DEFAULT_TIMESTAMP_PREFERENCE = ImmutableMap.of(
      ReservedName.SOURCE_TIME, 20,
      ReservedName.INGEST_TIME, 3,
      Name.system("timestamp"), 10,
      Name.system("time"), 8);

  public static final int ADDED_TIMESTAMP_SCORE = 100;

  protected static int getTimestampScore(Name columnName) {
    return DEFAULT_TIMESTAMP_PREFERENCE.entrySet().stream()
        .filter(e -> e.getKey().matches(columnName.getCanonical()))
        .map(Entry::getValue).findFirst().orElse(1);
  }

  public static Optional<Integer> getTimestampScore(Name columnName, RelDataType datatype) {
    if (!CalciteUtil.isTimestamp(datatype)) {
      return Optional.empty();
    }
    return Optional.of(getTimestampScore(columnName));
  }

  public static TimestampInference getTimestampInference(UniversalTable tblBuilder) {
    Preconditions.checkArgument(tblBuilder.getParent() == null,
        "Can only be invoked on root table");
    TimestampInference.ImportBuilder timestamp = TimestampInference.buildImport();
    tblBuilder.getAllIndexedFields().forEach(indexField -> {
      if (indexField.getField() instanceof UniversalTable.Column) {
        UniversalTable.Column column = (UniversalTable.Column) indexField.getField();
        Optional<Integer> score = getTimestampScore(column.getName(), column.getType());
        score.ifPresent(s -> timestamp.addImport(indexField.getIndex(), s));
      }
    });
    return timestamp.build();
  }
}
