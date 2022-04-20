//package ai.datasqrl.schema.operations;
//
//import ai.datasqrl.parse.tree.AstVisitor;
//import ai.datasqrl.parse.tree.DistinctAssignment;
//import ai.datasqrl.parse.tree.ExpressionAssignment;
//import ai.datasqrl.parse.tree.ImportDefinition;
//import ai.datasqrl.parse.tree.Node;
//import ai.datasqrl.plan.ImportLocalPlannerResult;
//import ai.datasqrl.plan.LocalPlannerResult;
//import org.apache.calcite.sql.SqlNode;
//
//public class OperationFactory extends AstVisitor<SchemaOperation, LocalPlannerResult> {
//
//  public SchemaOperation create(Node sqlNode, SqlNode node,
//      LocalPlannerResult plan) {
//    return sqlNode.accept(this, plan);
//  }
//
//  @Override
//  public SchemaOperation visitImportDefinition(ImportDefinition node, LocalPlannerResult result) {
//    ImportLocalPlannerResult importResult = (ImportLocalPlannerResult) result;
//
//    return new AddDatasetOp(importResult);
//  }
//
//  @Override
//  public SchemaOperation visitDistinctAssignment(DistinctAssignment node,
//      LocalPlannerResult context) {
//
//    return new AddQueryOp(node.getNamePath(), context);
//  }
//
//}
