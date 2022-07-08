package ai.datasqrl.plan.local.analyzer;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.SqrlCalciteBridge;
import ai.datasqrl.plan.calcite.SqrlType2Calcite;
import ai.datasqrl.plan.calcite.sqrl.table.LogicalBaseTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.QueryCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.SourceTableCalciteTable;
import ai.datasqrl.plan.local.analyzer.Analysis.TableVersion;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
import ai.datasqrl.plan.local.operations.SourceTableImportOp.RowType;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;

public class Generator extends QueryGenerator implements SqrlCalciteBridge {
  Planner planner;

  public Generator(Planner planner, Analysis analysis) {
    super(analysis);
    this.planner = planner;
  }

  public void generate(ScriptNode scriptNode) {
    for (Node statement : scriptNode.getStatements()) {
      statement.accept(this, null);
    }
  }

  @Override
  public SqlNode visitImportDefinition(ImportDefinition node, Scope context) {
    List<SourceTableImport> sourceTableImports = analysis.getImportSourceTables().get(node);
    Map<ai.datasqrl.schema.Table,SourceTableImportOp.RowType> tableTypes = analysis.getImportTableTypes().get(node);

    SourceTableImport tableImport = sourceTableImports.get(0);//todo: support import *;
    RelDataType rootType = new FlexibleTableConverter(tableImport.getSchema())
        .apply(new CalciteSchemaGenerator(planner.getTypeFactory()))
        .get();

    SourceTableCalciteTable sourceTable = new SourceTableCalciteTable(tableImport,
        rootType);

    Name datasetName = Name.system(tableImport.getTable().qualifiedName());

    this.tables.put(datasetName, sourceTable);


    List<ai.datasqrl.schema.Table> tables = new ArrayList<>();
    //Produce a Calcite row schema for each table in the nested hierarchy
    for (Map.Entry<ai.datasqrl.schema.Table, SourceTableImportOp.RowType> tableImp : tableTypes.entrySet()) {
      RelDataType logicalTableType = getLogicalTableType(tableImp.getKey(), tableImp.getValue());
      ai.datasqrl.schema.Table table = tableImp.getKey();

      LogicalBaseTableCalciteTable baseTable = new LogicalBaseTableCalciteTable(
          tableImport, logicalTableType, table.getPath());

      this.tables.put(table.getId(), baseTable);

      tables.add(table);
    }

    return null;
  }

  private RelDataType getLogicalTableType(ai.datasqrl.schema.Table table, RowType rowType) {
    SqrlType2Calcite typeConverter = planner.getTypeConverter();

    FieldInfoBuilder fieldBuilder = planner.getTypeFactory().builder().kind(StructKind.FULLY_QUALIFIED);
//    Preconditions.checkArgument(table.getFields().getIndexLength() == rowType.size(),
//        "Row sizes are not the same. {} {}", table.getFields().getIndexLength(), rowType.size());
    for (int i = 0; i < rowType.size(); i++) {
      SourceTableImportOp.ColumnType colType = rowType.get(i);
      RelDataType type = colType.getType().accept(typeConverter, null);
      fieldBuilder.add(table.getFields().atIndex(i).getName().getCanonical(), type).nullable(colType.isNotnull());
    }
    return fieldBuilder.build();
  }

  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignment node, Scope context) {
    //Options:
    /**
     * Convert to query:
     * 1. FROM table: get calcite table and copy over all columns
     * 2. SELECT *
     * 3. ROW NUM over
     * 4. LIMIT 1
     */

    /*
     * Create transformer?
     */

//    SqlNode distinct = node.accept(transformer, null);

    throw new RuntimeException("TBD");
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignment node, Scope context) {
    throw new RuntimeException("TBD");
  }

  @Override
  public SqlNode visitExpressionAssignment(ExpressionAssignment expressionAssignment, Scope context) {
    throw new RuntimeException("TBD");
  }

  @Override
  public SqlNode visitQueryAssignment(QueryAssignment queryAssignment, Scope context) {
    if (analysis.getExpressionStatements().contains(queryAssignment)) {
      throw new RuntimeException("TBD");
    } else {
      SqlNode sqlNode = queryAssignment.getQuery().accept(this, context);
      RelNode relNode = plan(sqlNode);
      System.out.println(relNode.explain());

      TableVersion tableVersion = analysis.getProducedTable().get(queryAssignment);

      this.tables.put(tableVersion.getTable().getId(), new QueryCalciteTable(relNode));
    }
    return null;
  }

  @SneakyThrows
  private RelNode plan(SqlNode sqlNode) {
    planner.refresh();
    planner.validate(sqlNode);

    RelRoot root = planner.rel(sqlNode);
    RelNode relNode = root.rel;
    System.out.println(relNode);

    return relNode;
  }

  @Override
  public Table getTable(Name sqrlTableName) {
    Table table = this.tables.get(sqrlTableName);
    if (table != null) {
      return table;
    }
    throw new RuntimeException("Could not find table " + sqrlTableName);
  }
}
