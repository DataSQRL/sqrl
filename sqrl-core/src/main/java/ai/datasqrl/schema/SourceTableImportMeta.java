package ai.datasqrl.schema;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.schema.Table;

import java.util.ArrayList;
import java.util.Map;

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
    boolean notnull;

  }

  public static class RowType extends ArrayList<ColumnType> {

  }

}
