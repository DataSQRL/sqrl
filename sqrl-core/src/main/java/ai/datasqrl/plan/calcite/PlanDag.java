package ai.datasqrl.plan.calcite;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.operations.AddColumnOp;
import ai.datasqrl.plan.local.operations.AddJoinDeclarationOp;
import ai.datasqrl.plan.local.operations.AddNestedTableOp;
import ai.datasqrl.plan.local.operations.AddRootTableOp;
import ai.datasqrl.plan.local.operations.MultipleUpdateOp;
import ai.datasqrl.plan.local.operations.SchemaOpVisitor;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.plan.local.operations.ScriptTableImportOp;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Table;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;

@AllArgsConstructor
public class PlanDag implements SqrlCalciteBridge, SchemaOpVisitor {
  protected Planner planner;
  protected final Map<Name, org.apache.calcite.schema.Table> tableMap = new HashMap<>();

  @Override
  public org.apache.calcite.schema.Table getTable(Name sqrlTableName) {
    return tableMap.get(sqrlTableName);
  }

  @Override
  @SneakyThrows
  public <T> T visit(AddColumnOp op) {
    SchemaCalciteTable table = (SchemaCalciteTable)tableMap.get(op.getTable().getId());

    SqlNode sqlNode = planner.convert(op.getNode());
    planner.validate(sqlNode);

    RelRoot root = planner.rel(sqlNode);
    RelNode relNode = root.rel;
    int index = relNode.getRowType().getFieldList().size() - 1;
    RelDataTypeField relField = relNode.getRowType().getFieldList().get(index);
    table.addField(relField);

    return null;
  }

  @Override
  public <T> T visit(AddJoinDeclarationOp op) {
    /* No-op, calcite is not aware of join declarations */
    return null;
  }

  @Override
  public <T> T visit(AddNestedTableOp op) {
    addTable(op.getTable().getId(), op.getNode());
    return null;
  }

  @Override
  public <T> T visit(AddRootTableOp op) {
    addTable(op.getTable().getId(), op.getNode());
    return null;
  }

  @Override
  public <T> T visit(MultipleUpdateOp op) {
    for (SchemaUpdateOp o : op.getOps()) {
      o.accept(this);
    }
    return null;
  }

  @Override
  public <T> T visit(ScriptTableImportOp op) {
    return null;
  }

  @Override
  public <T> T visit(SourceTableImportOp op) {
    SourceTableImport tableImport = op.getSourceTableImport();

    SchemaCalciteTable schemaCalciteTable =
        new SchemaCalciteTable(op.getTable().getHead().getRowType().getFieldList());

    for (Relationship relationship : op.getTable().getRelationships()) {
      if (relationship.getJoinType() == JoinType.CHILD) {
        Table table = relationship.getToTable();
        SourceTableImportOp child = new SourceTableImportOp(table, null);
        child.accept(this);
      }
    }

    tableMap.put(op.getTable().getId(), schemaCalciteTable);

    return null;
  }

  @SneakyThrows //todo remove sneakythrows and add error handling
  private void addTable(Name name, Node node) {
    SqlNode sqlNode = planner.convert(node);
    planner.validate(sqlNode);

    RelRoot root = planner.rel(sqlNode);

//    planner.transform(0, RelTraitSet.createEmpty(), root.rel);

    SchemaCalciteTable schemaCalciteTable = new SchemaCalciteTable(root.rel.getRowType().getFieldList());
    tableMap.put(name, schemaCalciteTable);
    planner.reset();
  }
}
