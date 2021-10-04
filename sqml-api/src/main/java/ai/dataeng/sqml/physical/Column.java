package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.analyzer.Field;
import lombok.Value;

@Value
public class Column {
  Field field;
  String columnName;
}
