package ai.datasqrl.schema.operations;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.plan.ImportLocalPlannerResult;
import ai.datasqrl.plan.LocalPlannerResult;

public class OperationFactory extends AstVisitor<SchemaOperation, LocalPlannerResult> {

  public SchemaOperation create(Node sqlNode, LocalPlannerResult plan) {
    return sqlNode.accept(this, plan);
  }

  @Override
  public SchemaOperation visitImportDefinition(ImportDefinition node, LocalPlannerResult result) {
    ImportLocalPlannerResult importResult = (ImportLocalPlannerResult) result;

    return new AddDatasetOp(importResult);
  }
}
