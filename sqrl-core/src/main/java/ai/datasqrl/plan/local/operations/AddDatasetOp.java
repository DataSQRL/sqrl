package ai.datasqrl.plan.local.operations;

import ai.datasqrl.schema.Table;
import java.util.List;
import lombok.Value;

@Value
public class AddDatasetOp implements SchemaUpdateOp {

  List<Table> tables;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
