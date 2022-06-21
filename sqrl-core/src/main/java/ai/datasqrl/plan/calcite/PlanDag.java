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
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;

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
    //TODO: validate not null and DateTime for timestamps

    SchemaCalciteTable table = (SchemaCalciteTable)tableMap.get(op.getTable().getId());

    planner.refresh();
    SqlNode sqlNode = planner.convert(op.getNode());
    System.out.println(sqlNode);
    planner.validate(sqlNode);

    RelRoot root = planner.rel(sqlNode);
    RelNode relNode = root.rel;
    int index = relNode.getRowType().getFieldList().size() - 1;
    RelDataTypeField relField = relNode.getRowType().getFieldList().get(index);
    table.addField(relField);
    if (op.isTimestamp()) {
      Preconditions.checkArgument(!relField.getType().isNullable() && relField.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              "Invalid data type for timestamp column: " + relField);
    }
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
    SqrlType2Calcite typeConverter = planner.getTypeConverter();

    //Produce a Calcite row schema for each table in the nested hierarchy
    for (Map.Entry<Table, SourceTableImportOp.RowType> tableImp : op.getTableTypes().entrySet()) {
      FieldInfoBuilder fieldBuilder = planner.getTypeFactory().builder().kind(StructKind.FULLY_QUALIFIED);
      Table table = tableImp.getKey();
      SourceTableImportOp.RowType rowType = tableImp.getValue();
      Preconditions.checkArgument(table.getFields().size()== rowType.size());
      for (int i = 0; i< rowType.size(); i++) {
        SourceTableImportOp.ColumnType colType = rowType.get(i);
        RelDataType type = colType.getType().accept(typeConverter, null);
        fieldBuilder.add(table.getFields().get(i).getName().getCanonical(), type).nullable(colType.isNotnull());
      }

      SchemaCalciteTable schemaCalciteTable =
              new SchemaCalciteTable(fieldBuilder.build().getFieldList());
      tableMap.put(table.getId(), schemaCalciteTable);

    }
    return null;
  }

  @SneakyThrows //todo remove sneakythrows and add error handling
  private void addTable(Name name, Node node) {
    planner.refresh();
    SqlNode sqlNode = planner.convert(node);
    planner.validate(sqlNode);

    RelRoot root = planner.rel(sqlNode);

//    planner.transform(0, RelTraitSet.createEmpty(), root.rel);

    SchemaCalciteTable schemaCalciteTable = new SchemaCalciteTable(root.rel.getRowType().getFieldList());
    tableMap.put(name, schemaCalciteTable);
  }

  public void apply(SchemaUpdateOp op) {
    op.accept(this);
  }
}
