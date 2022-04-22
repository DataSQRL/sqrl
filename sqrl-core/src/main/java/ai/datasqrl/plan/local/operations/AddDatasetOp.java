package ai.datasqrl.plan.local.operations;

import java.util.List;
import lombok.Value;

@Value
public class AddDatasetOp implements SchemaUpdateOp {

  List<ImportTable> importedPaths;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
