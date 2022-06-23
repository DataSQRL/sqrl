package ai.datasqrl.plan.calcite;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.sqrl.table.LogicalBaseTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.SourceTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.QueryCalciteTable;
import ai.datasqrl.plan.local.operations.AddColumnOp;
import ai.datasqrl.plan.local.operations.AddJoinDeclarationOp;
import ai.datasqrl.plan.local.operations.AddNestedTableOp;
import ai.datasqrl.plan.local.operations.AddRootTableOp;
import ai.datasqrl.plan.local.operations.MultipleUpdateOp;
import ai.datasqrl.plan.local.operations.SchemaOpVisitor;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.plan.local.operations.ScriptTableImportOp;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;

/**
 * This class is the minimal implementation to validate SQRL queries and infer data types.
 */
@AllArgsConstructor
public class BasicSqrlCalciteBridge implements SqrlCalciteBridge, SchemaOpVisitor {
  protected Planner planner;
  protected final Map<Name, AbstractTable> tableMap = new HashMap<>();

  @Override
  public org.apache.calcite.schema.Table getTable(Name sqrlTableName) {
    return tableMap.get(sqrlTableName);
  }

  /**
   * Adds the table definitions to the schema
   */
  @Override
  public <T> T visit(SourceTableImportOp op) {
    RelDataType rootType = new FlexibleTableConverter(op.getSourceTableImport().getSchema())
        .apply(new CalciteSchemaGenerator(planner.getTypeFactory()))
        .get();

    SourceTableCalciteTable sourceTable = new SourceTableCalciteTable(op.getSourceTableImport(), rootType);

    Name datasetName = Name.system(op.getSourceTableImport().getTable().qualifiedName());
    setTable(datasetName, sourceTable);

    LogicalBaseTableCalciteTable baseTable = new LogicalBaseTableCalciteTable(
        op.getSourceTableImport(), rootType, NamePath.of(op.getRootTable().getName()));

    setTable(op.getRootTable().getId(), baseTable);

    return (T)List.of(op.getRootTable());

  }

  @Override
  @SneakyThrows
  public <T> T visit(AddColumnOp op) {
    QueryCalciteTable query = planQuery(op.getJoinedNode());
    setTable(op.getTable().getId(), query);

    return null;
  }

  @Override
  public <T> T visit(AddJoinDeclarationOp op) {
    /* No-op, calcite is not aware of join declarations */
    return null;
  }

  @SneakyThrows
  @Override
  public <T> T visit(AddNestedTableOp op) {
    QueryCalciteTable query = planQuery(op.getNode());
    setTable(op.getTable().getId(), query);

    return null;
  }

  @Override
  @SneakyThrows
  public <T> T visit(AddRootTableOp op) {
    QueryCalciteTable query = planQuery(op.getNode());
    setTable(op.getTable().getId(), query);

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

  private void setTable(Name id, AbstractTable table) {
    this.tableMap.put(id, table);
  }

  private QueryCalciteTable planQuery(Node node) {
    return new QueryCalciteTable(plan(node));
  }

  @SneakyThrows
  private RelNode plan(Node node) {
    planner.refresh();
    SqlNode sqlNode = planner.convert(node);
    System.out.println(sqlNode);
    planner.validate(sqlNode);

    RelRoot root = planner.rel(sqlNode);
    RelNode relNode = root.rel;
    return relNode;
  }

  public void apply(SchemaUpdateOp op) {
    op.accept(this);
  }
}
