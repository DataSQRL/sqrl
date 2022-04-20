package ai.datasqrl.schema.operations;

import java.util.List;
import lombok.Value;

@Value
public class AddDatasetOp implements SchemaOperation {
  List<ImportTable> importedPaths;

  @Override
  public <T> T accept(OperationVisitor visitor) {
    return visitor.visit(this);
  }
}
