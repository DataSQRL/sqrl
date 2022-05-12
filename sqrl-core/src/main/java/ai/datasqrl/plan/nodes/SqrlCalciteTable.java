package ai.datasqrl.plan.nodes;

import ai.datasqrl.schema.Table;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;

public class SqrlCalciteTable extends RelDataTypeImpl {

  @Getter
  private final Table table;

  public SqrlCalciteTable(
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
