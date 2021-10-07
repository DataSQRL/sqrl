package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.schema2.Field;
import lombok.Value;

@Value
public class Column {
  Field field;
  String columnName;
}
