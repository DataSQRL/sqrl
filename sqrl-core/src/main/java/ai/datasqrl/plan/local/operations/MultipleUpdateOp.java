package ai.datasqrl.plan.local.operations;

import java.util.List;
import lombok.Value;

@Value
public class MultipleUpdateOp implements SchemaUpdateOp {
  List<SchemaUpdateOp> ops;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
