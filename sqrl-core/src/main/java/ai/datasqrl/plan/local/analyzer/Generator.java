package ai.datasqrl.plan.local.analyzer;

import static ai.datasqrl.plan.local.SqlNodeUtil.and;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.SortItem;
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
import ai.datasqrl.plan.local.SqlJoinDeclaration;
import ai.datasqrl.plan.local.SqlNodeUtil;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyzer.Analysis.TableVersion;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
import ai.datasqrl.plan.local.operations.SourceTableImportOp.RowType;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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
    for (ai.datasqrl.schema.Table table : tables){
      table.getAllRelationships().forEach(this::createParentChildJoinDeclaration);
    }

    return null;
  }

  private void createParentChildJoinDeclaration(Relationship rel) {
    this.getJoins().put(rel, new SqlJoinDeclaration(createTableRef(rel.getToTable()), createParentChildCondition(rel)));

  }

  private SqlNode createParentChildCondition(Relationship rel) {
    Name lhsName = rel.getJoinType().equals(JoinType.PARENT) ? rel.getFromTable().getId() : rel.getToTable().getId();
    Name rhsName = rel.getJoinType().equals(JoinType.PARENT) ? rel.getToTable().getId() : rel.getFromTable().getId();

    AbstractSqrlTable lhs = tables.get(lhsName);
    AbstractSqrlTable rhs = tables.get(rhsName);

    List<SqlNode> conditions = new ArrayList<>();
    for (String pk : lhs.getPrimaryKeys()) {
      conditions.add(new SqlBasicCall(
          SqrlOperatorTable.EQUALS,
          new SqlNode[]{
              new SqlIdentifier(List.of("_", pk), SqlParserPos.ZERO),
              new SqlIdentifier(List.of("t", pk), SqlParserPos.ZERO)
          },
          SqlParserPos.ZERO));
    }

    return and(conditions);
  }

  private SqlNode createTableRef(ai.datasqrl.schema.Table table) {

    return new SqlBasicCall(SqrlOperatorTable.AS,
        new SqlNode[]{
            new SqlTableRef(SqlParserPos.ZERO, new SqlIdentifier(table.getId().getCanonical(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
            new SqlIdentifier("t", SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
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

  /**
   * SELECT [column_list]
   * FROM (
   *    SELECT [column_list],
   *      ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
   *        ORDER BY time_attr [asc|desc]) AS rownum
   *    FROM table_name)
   * WHERE rownum = 1;
   */
  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignment node, Scope context) {
    SqlNode sqlNode = generateDistinctQuery(node);

    QueryCalciteTable table = new QueryCalciteTable(plan(sqlNode));
    TableVersion version = analysis.getProducedTable().get(node);
    this.tables.put(version.getId(), table);

    return sqlNode;
  }
  public SqlNode generateDistinctQuery(DistinctAssignment node) {
    ResolvedNamePath path = analysis.getResolvedNamePath().get(node.getTableNode());
    AbstractSqrlTable table = tables.get(path.getToTable().getId());
    Preconditions.checkNotNull(table, "Could not find table");

    List<SqlNode> inner = SqlNodeUtil.toSelectList(table.getRowType(null).getFieldList());
    List<SqlNode> outer = new ArrayList<>(inner);

    SqlBasicCall rowNum = new SqlBasicCall(SqrlOperatorTable.ROW_NUMBER, new SqlNode[]{}, SqlParserPos.ZERO);

    List<SqlNode> partition = node.getPartitionKeyNodes().stream()
        .map(n->analysis.getResolvedNamePath().get(n))
        .map(n->n.getPath().get(0))
        .map(n->SqlNodeUtil.fieldToNode(table.getField(n)))
        .collect(Collectors.toList());

    List<SqlNode> orderList = new ArrayList<>();
    if (!node.getOrder().isEmpty()) {
      for (SortItem sortItem : node.getOrder()) {
        orderList.add(sortItem.accept(this, null));
      }
    } else {
      //default to current timestamp
      orderList.add(SqlNodeUtil.fieldToNode(table.getTimestamp()));
    }

    SqlNode[] operands = {rowNum,
        new SqlWindow(pos.getPos(node.getLocation()), null, null, new SqlNodeList(partition, SqlParserPos.ZERO),
            new SqlNodeList(orderList, SqlParserPos.ZERO),
            SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())), null, null, null)};

    SqlBasicCall over = new SqlBasicCall(SqlStdOperatorTable.OVER, operands, pos.getPos(node.getLocation()));

    SqlBasicCall rowNumAlias = new SqlBasicCall(
        SqrlOperatorTable.AS,
        new SqlNode[]{
            over,
            new SqlIdentifier("_row_num", SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
    inner.add(rowNumAlias);

    SqlNode outerSelect = new SqlSelect(
        SqlParserPos.ZERO,
        null,
        new SqlNodeList(outer, SqlParserPos.ZERO),
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            new SqlNodeList(inner, SqlParserPos.ZERO),
            new SqlTableRef(SqlParserPos.ZERO,
                new SqlIdentifier(path.getToTable().getId().getCanonical(), SqlParserPos.ZERO), new SqlNodeList(SqlParserPos.ZERO)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new SqlNodeList(SqlParserPos.ZERO)
        ),
        new SqlBasicCall(
            SqrlOperatorTable.EQUALS,
            new SqlNode[]{
                new SqlIdentifier("_row_num", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
            },
            SqlParserPos.ZERO),
        null,
        null,
        null,
        null,
        null,
        null,
        new SqlNodeList(SqlParserPos.ZERO)
    );

    return outerSelect;
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignment node, Scope context) {
    //Create join declaration, recursively expand paths.
    SqlJoin sqlNode = (SqlJoin)node.getJoinDeclaration().getRelation().accept(this, null);
    SqlNodeUtil.printJoin(sqlNode);
    Relationship rel = (Relationship) analysis.getProducedField().get(node);
    this.getJoins().put(rel, new SqlJoinDeclaration(sqlNode.getRight(), sqlNode.getCondition()));
    //todo: plan/validate

    return sqlNode;
  }

  @Override
  public SqlNode visitExpressionAssignment(ExpressionAssignment node, Scope context) {

    TableVersion v = analysis.getProducedTable().get(node);
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
        RelDataTypeField produced = relNode.getRowType().getFieldList().get(relNode.getRowType().getFieldList().size() - 1);
        RelDataTypeField newExpr = new RelDataTypeFieldImpl(field.getId().getCanonical(),
            table.getRowType(null).getFieldList().size(),
            produced.getType());

        table.addField(field, newExpr);

        return ctx.getSubqueries().get(0);
      } else {
        throw new RuntimeException("TBD");
      }
    } else {
      //Simple
      Field field = analysis.getProducedField().get(node);

      SqlCall call = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{sqlNode, new SqlIdentifier(field.getId().getCanonical(), SqlParserPos.ZERO)}, SqlParserPos.ZERO);

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null,
          new SqlNodeList(List.of(call), SqlParserPos.ZERO),
          new SqlTableRef(SqlParserPos.ZERO, new SqlIdentifier(v.getTable().getId().getCanonical(), SqlParserPos.ZERO),  SqlNodeList.EMPTY),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          SqlNodeList.EMPTY);
      System.out.println(select);
      RelNode relNode = plan(select);
      AbstractSqrlTable table = this.tables.get(v.getId());
      RelDataTypeField produced = relNode.getRowType().getFieldList().get(0);
      RelDataTypeField newExpr = new RelDataTypeFieldImpl(produced.getName(),
          table.getRowType(null).getFieldList().size(),
          produced.getType());

      table.addField(field, newExpr);

      return select.getSelectList().get(0); //fully validated node
    }
  }

  @Override
  public SqlNode visitQueryAssignment(QueryAssignment queryAssignment, Scope context) {
    if (analysis.getExpressionStatements().contains(queryAssignment)) {
      throw new RuntimeException("TBD");
    } else {
      SqlNode sqlNode = queryAssignment.getQuery().accept(this, new Scope(null, null));
      System.out.println(sqlNode);
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
