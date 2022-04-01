package ai.dataeng.sqml.parser.sqrl.schema;

import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import lombok.Getter;
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

    @Getter
    private final Table table;

    public StreamDataType(
        Table table, List<? extends RelDataTypeField> fieldList) {
      super(fieldList);
      this.table = table;
      computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean b) {
      sb.append("(DynamicRecordRow")
          .append(getFieldNames())
          .append(")");
    }
  }
}
