package ai.datasqrl.plan.nodes;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
}
