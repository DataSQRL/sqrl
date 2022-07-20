package ai.datasqrl.schema;

import ai.datasqrl.environment.ImportManager.SourceTableImport;

import java.util.ArrayList;
import java.util.Map;

import ai.datasqrl.schema.type.Type;
import lombok.Value;

@Value
public class SourceTableImportMeta {
  VarTable rootTable;
  Map<VarTable, RowType> tableTypes;
  SourceTableImport sourceTableImport;

  @Value
  public static class ColumnType {

    Type type;
    boolean nullable;

  }

  public static class RowType extends ArrayList<ColumnType> {

  }

}
