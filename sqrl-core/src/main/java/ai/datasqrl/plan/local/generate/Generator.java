package ai.datasqrl.plan.local.generate;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.SqrlStatement;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.SqrlCalciteBridge;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.SqrlType2Calcite;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.LogicalBaseTableCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.QueryCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.SourceTableCalciteTable;
import ai.datasqrl.plan.local.analyze.Analysis;
import ai.datasqrl.plan.local.generate.node.SqlJoinDeclaration;
import ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SourceTableImportMeta;
import ai.datasqrl.schema.SourceTableImportMeta.RowType;
import ai.datasqrl.schema.Table;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParserPos;

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

  public SqlNode generate(SqrlStatement statement) {
    return statement.accept(this, null);
  }

  @Override
  public SqlNode visitImportDefinition(ImportDefinition node, Scope context) {
    List<SourceTableImport> sourceTableImports = analysis.getImportSourceTables().get(node);
    Map<ai.datasqrl.schema.Table, SourceTableImportMeta.RowType> tableTypes =
        analysis.getImportTableTypes()
        .get(node);

    SourceTableImport tableImport = sourceTableImports.get(0);//todo: support import *;
    RelDataType rootType = new FlexibleTableConverter(tableImport.getSchema()).apply(
        new CalciteSchemaGenerator(planner.getTypeFactory())).get();

    SourceTableCalciteTable sourceTable = new SourceTableCalciteTable(tableImport, rootType);

    Name datasetName = Name.system(tableImport.getTable().qualifiedName());

    this.tables.put(datasetName, sourceTable);

    List<ai.datasqrl.schema.Table> tables = new ArrayList<>();
    //Produce a Calcite row schema for each table in the nested hierarchy
    for (Map.Entry<ai.datasqrl.schema.Table, SourceTableImportMeta.RowType> tableImp :
        tableTypes.entrySet()) {
      RelDataType logicalTableType = getLogicalTableType(tableImp.getKey(), tableImp.getValue());
      ai.datasqrl.schema.Table table = tableImp.getKey();

      LogicalBaseTableCalciteTable baseTable = new LogicalBaseTableCalciteTable(tableImport,
          logicalTableType, table.getPath());

      this.tables.put(table.getId(), baseTable);

      tables.add(table);
    }
    for (ai.datasqrl.schema.Table table : tables) {
      table.getAllRelationships().forEach(this::createParentChildJoinDeclaration);
    }

    return null;
  }

  private RelDataType getLogicalTableType(ai.datasqrl.schema.Table table, RowType rowType) {
    SqrlType2Calcite typeConverter = planner.getTypeConverter();

    FieldInfoBuilder fieldBuilder = planner.getTypeFactory().builder()
        .kind(StructKind.FULLY_QUALIFIED);
//    Preconditions.checkArgument(table.getFields().getIndexLength() == rowType.size(),
//        "Row sizes are not the same. {} {}", table.getFields().getIndexLength(), rowType.size());
    for (int i = 0; i < rowType.size(); i++) {
      SourceTableImportMeta.ColumnType colType = rowType.get(i);
      RelDataType type = colType.getType().accept(typeConverter, null);
      fieldBuilder.add(table.getFields().atIndex(i).getName().getCanonical(), type)
          .nullable(colType.isNotnull());
    }
    return fieldBuilder.build();
  }

  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignment node, Scope context) {
    SqlNode sqlNode = generateDistinctQuery(node);
    RelNode relNode = plan(sqlNode);
    QueryCalciteTable table = new QueryCalciteTable(relNode);
    Table createdTable = analysis.getProducedTable().get(node);
    this.tables.put(createdTable.getId(), table);

    return sqlNode;
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignment node, Scope context) {
    //Create join declaration, recursively expand paths.
    SqlJoin sqlNode = (SqlJoin) node.getJoinDeclaration().getRelation().accept(this, null);
    SqlNodeUtil.printJoin(sqlNode);
    Relationship rel = (Relationship) analysis.getProducedField().get(node);
    this.getJoins().put(rel, new SqlJoinDeclaration(sqlNode.getRight(), sqlNode.getCondition()));
    //todo: plan/validate

    return sqlNode;
  }

  @Override
  public SqlNode visitExpressionAssignment(ExpressionAssignment node, Scope context) {

    Table v = analysis.getProducedTable().get(node);
    //Were do subqueries live here?
    Scope ctx = new Scope(this.analysis, this);
    SqlNode sqlNode = node.getExpression().accept(this, ctx);

    if (!ctx.getSubqueries().isEmpty()) {
      //With subqueries
      if (ctx.getSubqueries().size() > 1) {
        throw new RuntimeException("TBD");
      } else if (sqlNode instanceof SqlIdentifier) {
        //Just one subquery and just a literal assignment. No need to rejoin to parent.
        //e.g. Orders.total := sum(entries.total);

        RelNode relNode = plan(ctx.getSubqueries().get(0));
        Field field = analysis.getProducedField().get(node);

        AbstractSqrlTable table = this.tables.get(v.getId());
        RelDataTypeField produced = relNode.getRowType().getFieldList()
            .get(relNode.getRowType().getFieldList().size() - 1);
        RelDataTypeField newExpr = new RelDataTypeFieldImpl(field.getId().getCanonical(),
            table.getRowType(null).getFieldList().size(), produced.getType());

        table.addField(newExpr);

        return ctx.getSubqueries().get(0);
      } else {
        throw new RuntimeException("TBD");
      }
    } else {
      //Simple
      Field field = analysis.getProducedField().get(node);

      SqlCall call = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{sqlNode,
          new SqlIdentifier(field.getId().getCanonical(), SqlParserPos.ZERO)}, SqlParserPos.ZERO);

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null,
          new SqlNodeList(List.of(call), SqlParserPos.ZERO), new SqlBasicCall(SqrlOperatorTable.AS,
          new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
              new SqlIdentifier(v.getId().getCanonical(), SqlParserPos.ZERO),
              SqlNodeList.EMPTY), new SqlIdentifier("_", SqlParserPos.ZERO)}, SqlParserPos.ZERO),
          null, null, null, null, null, null, null, SqlNodeList.EMPTY);
      RelNode relNode = plan(select);
      AbstractSqrlTable table = this.tables.get(v.getId());
      RelDataTypeField produced = relNode.getRowType().getFieldList().get(0);
      RelDataTypeField newExpr = new RelDataTypeFieldImpl(produced.getName(),
          table.getRowType(null).getFieldList().size(), produced.getType());

      table.addField(newExpr);

      return select.getSelectList().get(0); //fully validated node
    }
  }

  @Override
  public SqlNode visitQueryAssignment(QueryAssignment queryAssignment, Scope context) {
    if (analysis.getExpressionStatements().contains(queryAssignment)) {
      SqlNode sqlNode = queryAssignment.getQuery().accept(this, new Scope(null, null));
      RelNode relNode = plan(sqlNode);
      Table table = analysis.getProducedTable().get(queryAssignment);
      Preconditions.checkNotNull(table);
      AbstractSqrlTable t = this.tables.get(table.getId());
      RelDataTypeField field = relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount() - 1);
      RelDataTypeField newField = new RelDataTypeFieldImpl(
          queryAssignment.getNamePath().getLast().getCanonical(),
          t.getRowType(null).getFieldCount(), field.getValue());

      t.addField(newField);

      return sqlNode;
    } else {
      SqlNode sqlNode = queryAssignment.getQuery().accept(this, new Scope(null, null));
      RelNode relNode = plan(sqlNode);

      Table table = analysis.getProducedTable().get(queryAssignment);

      this.tables.put(table.getId(), new QueryCalciteTable(relNode));
    }
    return null;
  }

  @SneakyThrows
  private RelNode plan(SqlNode sqlNode) {
    planner.refresh();
    planner.validate(sqlNode);

    RelRoot root = planner.rel(sqlNode);
    RelNode relNode = root.rel;

    return relNode;
  }

  @Override
  public org.apache.calcite.schema.Table getTable(Name sqrlTableName) {
    org.apache.calcite.schema.Table table = this.tables.get(sqrlTableName);
    if (table != null) {
      return table;
    }
    throw new RuntimeException("Could not find table " + sqrlTableName);
  }
}
