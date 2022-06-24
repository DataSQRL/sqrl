package ai.datasqrl.plan.calcite;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
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
import ai.datasqrl.plan.local.operations.SourceTableImportOp.RowType;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.StructKind;
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
   * In order to expose a hierarchical table in Calcite, we need to register the source dataset
   * with the full calcite schema and a table for each nested record, which we can then query.
   *
   * To do this, we use a process called table shredding. This involves removing the nested
   * records from the source dataset, and then registering the resulting table with the calcite
   * schema.
   *
   * We can then expand this into a full logical plan using the
   * {@link ai.datasqrl.plan.calcite.sqrl.rules.SqrlExpansionRelRule}
   */
  @Override
  public <T> T visit(SourceTableImportOp op) {
    RelDataType rootType = new FlexibleTableConverter(op.getSourceTableImport().getSchema())
        .apply(new CalciteSchemaGenerator(planner.getTypeFactory()))
        .get();

    SourceTableCalciteTable sourceTable = new SourceTableCalciteTable(op.getSourceTableImport(),
        rootType);

    Name datasetName = Name.system(op.getSourceTableImport().getTable().qualifiedName());
    setTable(datasetName, sourceTable);


    List<Table> tables = new ArrayList<>();
    //Produce a Calcite row schema for each table in the nested hierarchy
    for (Map.Entry<Table, SourceTableImportOp.RowType> tableImp : op.getTableTypes().entrySet()) {
      RelDataType logicalTableType = getLogicalTableType(tableImp.getKey(), tableImp.getValue());
      Table table = tableImp.getKey();

      LogicalBaseTableCalciteTable baseTable = new LogicalBaseTableCalciteTable(
          op.getSourceTableImport(), logicalTableType, table.getPath());

      setTable(table.getId(), baseTable);

      tables.add(table);
    }

    return (T) tables;
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

  private RelDataType getLogicalTableType(Table table, RowType rowType) {
    SqrlType2Calcite typeConverter = planner.getTypeConverter();

    FieldInfoBuilder fieldBuilder = planner.getTypeFactory().builder().kind(StructKind.FULLY_QUALIFIED);
    Preconditions.checkArgument(table.getFields().getIndexLength() == rowType.size());
    for (int i = 0; i < rowType.size(); i++) {
      SourceTableImportOp.ColumnType colType = rowType.get(i);
      RelDataType type = colType.getType().accept(typeConverter, null);
      fieldBuilder.add(table.getFields().atIndex(i).getName().getCanonical(), type).nullable(colType.isNotnull());
    }
    return fieldBuilder.build();
  }

  public void apply(SchemaUpdateOp op) {
    op.accept(this);
  }
}
