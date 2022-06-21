package ai.datasqrl.plan.local.operations;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.schema.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.datasqrl.schema.type.Type;
import lombok.Value;

@Value
public class SourceTableImportOp implements SchemaUpdateOp {
  Table rootTable;
  Map<Table, RowType> tableTypes;
  SourceTableImport sourceTableImport;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }

  @Value
  public static class ColumnType {

    Type type;
    boolean notnull;

  }

  public static class RowType extends ArrayList<ColumnType> {

  }

}
