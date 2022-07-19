package ai.datasqrl.plan.local.generate;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.*;
import ai.datasqrl.plan.calcite.sqrl.rules.Sqrl2SqlLogicalPlanConverter;
import ai.datasqrl.plan.calcite.sqrl.table.*;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import ai.datasqrl.plan.local.analyze.Analysis;
import ai.datasqrl.plan.local.generate.node.SqlJoinDeclaration;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SourceTableImportMeta;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.table.VirtualTableFactory;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Generator extends QueryGenerator implements SqrlCalciteBridge {

  private final CalciteTableFactory calciteFactory;
  Planner planner;

  public Generator(Planner planner, Analysis analysis) {
    super(analysis);
    this.planner = planner;
    this.calciteFactory = new CalciteTableFactory();
  }

  public void generate(ScriptNode scriptNode) {
    for (Node statement : scriptNode.getStatements()) {
      statement.accept(this, null);
    }
  }

  public SqlNode generate(SqrlStatement statement) {
    return statement.accept(this, null);
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
  public SqlNode visitImportDefinition(ImportDefinition node, Scope context) {
    List<SourceTableImport> sourceTableImports = analysis.getImportSourceTables().get(node);
    Map<ai.datasqrl.schema.Table, SourceTableImportMeta.RowType> tableTypes =
        analysis.getImportTableTypes()
        .get(node);

    SourceTableImport tableImport = sourceTableImports.get(0);//todo: support import *;
    CalciteSchemaGenerator schemaGen = new CalciteSchemaGenerator(planner.getTypeFactory(), calciteFactory);
    RelDataType rootType = new FlexibleTableConverter(tableImport.getSchema()).apply(schemaGen).get();

    VirtualTableFactory.TableBuilder<RelDataType> rootTable = schemaGen.getRootTable();

    ImportedSqrlTable impTable = new ImportedSqrlTable(tableImport.getTable().getName(), tableImport, rootType);

    Name datasetName = Name.system(tableImport.getTable().qualifiedName());

    this.tables.put(datasetName.getCanonical(), impTable);

    List<ai.datasqrl.schema.Table> tables = new ArrayList<>();
    //Produce a Calcite row schema for each table in the nested hierarchy
    for (Map.Entry<ai.datasqrl.schema.Table, SourceTableImportMeta.RowType> tableImp :
        tableTypes.entrySet()) {
      RelDataType logicalTableType = getLogicalTableType(tableImp.getKey(), tableImp.getValue());
      ai.datasqrl.schema.Table table = tableImp.getKey();

      RelBuilder relBuilder = planner.getRelBuilder();
      relBuilder.scan(datasetName.getCanonical());
      RelNode relNode = relBuilder.build(); //?

      for (int i = 0; i < logicalTableType.getFieldList().size(); i++) {
        this.fieldNames.put(table.getFields().values().get(i),
            logicalTableType.getFieldList().get(i).getName());
      }
      System.out.println(this.fieldNames);

      TimestampHolder timeHolder = new TimestampHolder();
      QuerySqrlTable queryTable = new QuerySqrlTable(rootTable.getId(), QuerySqrlTable.Type.STREAM, logicalTableType,
          timeHolder, rootTable.getNumPrimaryKeys());
//      Set timestamp candidates
      calciteFactory.getTimestampCandidateScores(rootTable).entrySet().stream().forEach( e -> {
        RelDataTypeField field = impTable.getField(e.getKey().getId());
        timeHolder.addCandidate(field.getIndex(),e.getValue());
      });

      //?
//      List<VirtualSqrlTable> vtables = calciteFactory.createProxyTables(rootTable, queryTable, schemaGen).stream()
//          .map(TableProxy::getVirtualTable).collect(Collectors.toList());
//      vtables.stream().forEach(vt -> this.tables.put(Name.system(vt.getNameId()), vt));
      this.tableMap.put(table, queryTable);
      this.tables.put(queryTable.getNameId(), queryTable);

      tables.add(table);
    }
    for (ai.datasqrl.schema.Table table : tables) {
      table.getAllRelationships().forEach(this::createParentChildJoinDeclaration);
    }

    return null;
  }
//  private TableProxy<VirtualSqrlTable> toTableProxy(Table table) {
//    return tableFactory.walkTable(table.getPath()).orElseThrow();
//  }
  private RelDataType getLogicalTableType(ai.datasqrl.schema.Table table, SourceTableImportMeta.RowType rowType) {
    SqrlType2Calcite typeConverter = planner.getTypeConverter();

    FieldInfoBuilder fieldBuilder = planner.getTypeFactory().builder()
        .kind(StructKind.FULLY_QUALIFIED);
//    Preconditions.checkArgument(table.getFields().getIndexLength() == rowType.size(),
//        "Row sizes are not the same. {} {}", table.getFields().getIndexLength(), rowType.size());
    for (int i = 0; i < rowType.size(); i++) {
      SourceTableImportMeta.ColumnType colType = rowType.get(i);
      RelDataType type = colType.getType().accept(typeConverter, null);
      fieldBuilder.add(table.getFields().atIndex(i).getName().getCanonical(), type)
          .nullable(colType.isNullable());
    }
    return fieldBuilder.build();
  }

  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignment node, Scope context) {
    SqlNode sqlNode = generateDistinctQuery(node);
    //Field names are the same...
    Table table = analysis.getProducedTable().get(node);

    RelNode relNode = plan(sqlNode);
    for (int i = 0; i < analysis.getProducedFieldList().get(node).size(); i++) {
      this.fieldNames.put(analysis.getProducedFieldList().get(node).get(i),
          relNode.getRowType().getFieldList().get(i).getName());
    }

    QueryCalciteTable queryTable = new QueryCalciteTable(relNode);
    this.tables.put(queryTable.getNameId(), queryTable);
    this.tableMap.put(table, queryTable);
    return sqlNode;
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignment node, Scope context) {
    //Create join declaration, recursively expand paths.
    Table table = analysis.getParentTable().get(node);
    AbstractSqrlTable tbl = tableMap.get(table);
    Scope scope = new Scope(Optional.ofNullable(tbl), true);
    SqlJoin sqlNode = (SqlJoin) node.getJoinDeclaration().getRelation().accept(this, scope);
    //SqlNodeUtil.printJoin(sqlNode);
    Relationship rel = (Relationship) analysis.getProducedField().get(node);

    this.fieldNames.put(rel, rel.getName().getCanonical());
    this.getJoins()
        .put(rel, new SqlJoinDeclaration(sqlNode.getRight(), Optional.of(sqlNode.getCondition())));
    //todo: plan/validate

    return sqlNode;
  }

  @Override
  public SqlNode visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    Table v = analysis.getProducedTable().get(node);
    Table ta = analysis.getParentTable().get(node);
    AbstractSqrlTable tbl = tableMap.get(ta);
    Scope ctx = new Scope(Optional.ofNullable(tbl), true);
    SqlNode sqlNode = node.getExpression().accept(this, ctx);

    if (!ctx.getAddlJoins().isEmpty()) {
      //With subqueries
      if (ctx.getAddlJoins().size() > 1) {
        throw new RuntimeException("TBD");
      } else if (sqlNode instanceof SqlIdentifier) {
        //Just one subquery and just a literal assignment. No need to rejoin to parent.
        //e.g. Orders.total := sum(entries.total);
        //AS
        SqlBasicCall call = (SqlBasicCall) ctx.getAddlJoins().get(0).getRel();
        RelNode relNode = plan(call.getOperandList().get(0));
        Field field = analysis.getProducedField().get(node);
        this.fieldNames.put(field, relNode.getRowType().getFieldList()
            .get(relNode.getRowType().getFieldCount()-1).getName());


        AbstractSqrlTable table = this.tableMap.get(v);
        RelDataTypeField produced = relNode.getRowType().getFieldList()
            .get(relNode.getRowType().getFieldList().size() - 1);
        RelDataTypeField newExpr = new RelDataTypeFieldImpl(fieldNames.get(field),
            table.getRowType(null).getFieldList().size(), produced.getType());

        table.addField(newExpr);

        return ctx.getAddlJoins().get(0).getRel();
      } else {
        throw new RuntimeException("TBD");
      }
    } else {
      //Simple
      AbstractSqrlTable t = tableMap.get(v);
      Field field = analysis.getProducedField().get(node);

      SqlCall call = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{sqlNode,
          new SqlIdentifier(getUniqueName(t, node.getNamePath().getLast().getCanonical()), SqlParserPos.ZERO)}, SqlParserPos.ZERO);

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null,
          new SqlNodeList(List.of(call), SqlParserPos.ZERO), new SqlBasicCall(SqrlOperatorTable.AS,
          new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
              new SqlIdentifier(tableMap.get(v).getNameId(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
              new SqlIdentifier("_", SqlParserPos.ZERO)}, SqlParserPos.ZERO), null, null, null,
          null, null, null, null, SqlNodeList.EMPTY);
      System.out.println(select);

      RelNode relNode = plan(select);
      this.fieldNames.put(field, relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount()-1).getName());
      AbstractSqrlTable table = this.tableMap.get(v);
      RelDataTypeField produced = relNode.getRowType().getFieldList().get(0);
      RelDataTypeField newExpr = new RelDataTypeFieldImpl(produced.getName(),
          table.getRowType(null).getFieldList().size(), produced.getType());

      table.addField(newExpr);

      return select.getSelectList().get(0); //fully validated node
    }
  }

  private String getUniqueName(AbstractSqrlTable t, String newName) {
    List<String> toUnique = new ArrayList(t.getRowType().getFieldNames());
    toUnique.add(newName);
    List<String> uniqued = SqlValidatorUtil.uniquify(toUnique, false);
    return uniqued.get(uniqued.size()-1);
  }

  @Override
  public SqlNode visitQueryAssignment(QueryAssignment node, Scope context) {
    Table ta = analysis.getParentTable().get(node);
    Optional<AbstractSqrlTable> tbl = (ta == null) ? Optional.empty() :
        Optional.ofNullable(tableMap.get(ta));

    Scope scope = new Scope(tbl, true);
    SqlNode sqlNode = node.getQuery().accept(this, scope);
    RelNode relNode = plan(sqlNode);
    for (int i = 0; i < analysis.getProducedFieldList().get(node).size(); i++) {
      this.fieldNames.put(analysis.getProducedFieldList().get(node).get(i),
          relNode.getRowType().getFieldList().get(i + scope.getPPKOffset()).getName());
    }
    relNode = optimize(relNode);

    Table table = analysis.getProducedTable().get(node);

    if (analysis.getExpressionStatements().contains(node)) {
      Preconditions.checkNotNull(table);
      AbstractSqrlTable t = this.tableMap.get(table);
      RelDataTypeField field = relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount() - 1);
      RelDataTypeField newField = new RelDataTypeFieldImpl(
          node.getNamePath().getLast().getCanonical(), t.getRowType(null).getFieldCount(),
          field.getValue());

      t.addField(newField);

      return sqlNode;
    } else {
      QueryCalciteTable queryTable = new QueryCalciteTable(relNode);
      this.tables.put(queryTable.getNameId(), queryTable);
      this.tableMap.put(table, queryTable);
    }

    Relationship rel = (Relationship) analysis.getProducedField().get(node);
    if (rel == null) return null;
    //todo fix me
    Preconditions.checkNotNull(rel);
    this.getJoins().put(rel, new SqlJoinDeclaration(new SqlBasicCall(SqrlOperatorTable.AS,
        new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
            new SqlIdentifier(tableMap.get(table).getNameId(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
            //todo fix
            new SqlIdentifier("_internal$1", SqlParserPos.ZERO)}, SqlParserPos.ZERO),
        Optional.empty()));

    if (table.getField(ReservedName.PARENT).isPresent()) {
      Relationship relationship = (Relationship) table.getField(ReservedName.PARENT).get();
      this.getJoins().put(relationship, new SqlJoinDeclaration(
          new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
              new SqlIdentifier(tableMap.get(relationship.getToTable()).getNameId(),
                  SqlParserPos.ZERO), SqlNodeList.EMPTY),
              new SqlIdentifier(fieldNames.get(relationship), SqlParserPos.ZERO)},
              SqlParserPos.ZERO), Optional.empty()));
    }

    //add parent relationsihp

    return null;
  }

  public RelNode optimize(RelNode relNode) {
    System.out.println("LP$0: \n"+relNode.explain());

    //Step 1: Push filters into joins so we can correctly identify self-joins
    relNode = planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN, relNode);
    System.out.println("LP$1: \n"+relNode.explain());

    //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
    //self-joins (including nested self-joins) as well as infer primary keys,
    //table types, and timestamps in the process

    Sqrl2SqlLogicalPlanConverter sqrl2sql = new Sqrl2SqlLogicalPlanConverter(() -> planner.getRelBuilder(),
            new SqrlRexUtil(planner.getRelBuilder().getRexBuilder()));
    relNode = relNode.accept(sqrl2sql);
    System.out.println("LP$2: \n"+relNode.explain());
    return relNode;
  }

  @SneakyThrows
  private RelNode plan(SqlNode sqlNode) {
    planner.refresh();
    try {
      planner.validate(sqlNode);
    } catch (Exception e) {
      System.out.println(sqlNode.toString());
      throw new RuntimeException(e);
    }

    RelRoot root = planner.rel(sqlNode);
    RelNode relNode = root.rel;

    return relNode;
  }

  @Override
  public org.apache.calcite.schema.Table getTable(String tableName) {
    org.apache.calcite.schema.Table table = this.tables.get(tableName);
    if (table != null) {
      return table;
    }
    throw new RuntimeException("Could not find table " + tableName);
  }
}
