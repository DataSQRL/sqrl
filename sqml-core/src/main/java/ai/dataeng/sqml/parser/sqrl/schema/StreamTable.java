package ai.dataeng.sqml.parser.sqrl.schema;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.impl.AbstractTable;

public class StreamTable extends AbstractTable {

  RelDataTypeImpl relDataType;
  public StreamTable(RelDataTypeImpl relDataType) {
    this.relDataType = relDataType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }


  public static class StreamDataType extends RelDataTypeImpl {
    public StreamDataType(
        List<? extends RelDataTypeField> fieldList) {
      super(fieldList);
    }

    @Override
    protected void generateTypeString(StringBuilder stringBuilder, boolean b) {

    }
  }
}
