package ai.dataeng.sqml.parser.sqrl.schema;

import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager.SourceTableImport;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.impl.AbstractTable;

public class StreamTable extends AbstractTable {

  RelDataTypeImpl relDataType;
  @Getter
  private final SourceTableImport tableImport;

  public StreamTable(RelDataTypeImpl relDataType,
      SourceTableImport sourceTableImport) {
    this.relDataType = relDataType;
    this.tableImport = sourceTableImport;
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
