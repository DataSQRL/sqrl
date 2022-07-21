package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;

public class ImportedSqrlTable extends QuerySqrlTable {

  @Getter
  private final SourceTableImport sourceTableImport;
  private final RelDataType baseRowType;

  public ImportedSqrlTable(@NonNull Name rootTableId, @NonNull TimestampHolder timestamp,
                           SourceTableImport sourceTableImport, RelDataType rowType) {
    super(rootTableId, Type.STREAM, null, timestamp, 1);
    this.sourceTableImport = sourceTableImport;
    this.baseRowType = rowType;
  }

  public RelDataType getRowType() {
    if (relNode==null) return baseRowType;
    else return super.getRowType();
  }

}
