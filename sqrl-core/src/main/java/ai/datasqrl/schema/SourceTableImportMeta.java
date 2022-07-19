package ai.datasqrl.schema;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.sqrl.table.ImportedSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
import ai.datasqrl.schema.Table;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import ai.datasqrl.schema.type.Type;
import lombok.Value;

@Value
public class SourceTableImportMeta {
  Table rootTable;
  Map<Table, RowType> tableTypes;
  SourceTableImport sourceTableImport;

  @Value
  public static class ColumnType {

    Type type;
    boolean nullable;

  }

  public static class RowType extends ArrayList<ColumnType> {

  }

}
