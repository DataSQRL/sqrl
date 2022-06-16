package ai.datasqrl.plan.local.operations;

public interface SchemaOpVisitor {
  <T> T visit(AddColumnOp op);
  <T> T visit(AddJoinDeclarationOp op);
  <T> T visit(AddNestedTableOp op);
  <T> T visit(AddRootTableOp op);
  <T> T visit(MultipleUpdateOp op);
  <T> T visit(ScriptTableImportOp op);
  <T> T visit(SourceTableImportOp op);
}
