package ai.datasqrl.plan.local.operations;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.schema.Table;
import java.util.List;
import lombok.Value;

@Value
public class SourceTableImportOp implements SchemaUpdateOp {
  Table table;
  SourceTableImport sourceTableImport;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
