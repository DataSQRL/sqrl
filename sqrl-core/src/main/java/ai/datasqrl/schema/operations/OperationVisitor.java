package ai.datasqrl.schema.operations;

import ai.datasqrl.schema.operations.AddDatasetOp;

public abstract class OperationVisitor {

  public <T> T visit(AddDatasetOp op) {
    return null;
  }
}
