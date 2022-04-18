package ai.datasqrl.schema.operations;

import ai.datasqrl.plan.ImportLocalPlannerResult;
import lombok.Value;

@Value
public class AddDatasetOp implements SqrlOperation {
  ImportLocalPlannerResult result;

  @Override
  public <T> T accept(OperationVisitor visitor) {
    return visitor.visit(this);
  }
}
