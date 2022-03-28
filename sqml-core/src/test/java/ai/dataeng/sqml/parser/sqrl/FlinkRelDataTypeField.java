package ai.dataeng.sqml.parser.sqrl;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.flink.table.types.AbstractDataType;

public class FlinkRelDataTypeField extends RelDataTypeFieldImpl {

  public FlinkRelDataTypeField(String name, int index, RelDataType type) {
    super(name, index, type);
  }
}
