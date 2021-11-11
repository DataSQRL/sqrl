package ai.dataeng.sqml.graphql;

import ai.dataeng.sqml.schema2.TypedField;
import lombok.Value;

@Value
public class Column3 {
  String name;
  TypedField field;
  ColumnType type;
}
