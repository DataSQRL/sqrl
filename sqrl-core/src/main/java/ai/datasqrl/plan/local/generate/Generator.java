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
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.SqrlCalciteBridge;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.SqrlType2Calcite;
import ai.datasqrl.plan.calcite.sqrl.rules.Sqrl2SqlLogicalPlanConverter;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.CalciteTableFactory;
import ai.datasqrl.plan.calcite.sqrl.table.ImportedSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.QueryCalciteTable;
import ai.datasqrl.plan.calcite.sqrl.table.QuerySqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.TimestampHolder;
import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import ai.datasqrl.plan.local.analyze.Analysis;
import ai.datasqrl.plan.local.generate.node.SqlJoinDeclaration;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SourceTableImportMeta;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.table.TableProxy;
import ai.datasqrl.schema.table.TableProxyFactory;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
import org.apache.calcite.tools.RelBuilder;

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

    TableProxyFactory.TableBuilder<RelDataType> rootTable = schemaGen.getRootTable();

    ImportedSqrlTable impTable = new ImportedSqrlTable(tableImport.getTable().getName(), tableImport, rootType);

    Name datasetName = Name.system(tableImport.getTable().qualifiedName());

    this.tables.put(datasetName, impTable);

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

      this.tables.put(table.getId(), queryTable);

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
    Table createdTable = analysis.getProducedTable().get(node);

    RelNode relNode = plan(sqlNode);
    for (int i = 0; i < analysis.getProducedFieldList().get(node).size(); i++) {
      this.fieldNames.put(analysis.getProducedFieldList().get(node).get(i),
          relNode.getRowType().getFieldList().get(i).getName());
    }

    QueryCalciteTable table = new QueryCalciteTable(relNode);
    this.tables.put(createdTable.getId(), table);

    return sqlNode;
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignment node, Scope context) {
    //Create join declaration, recursively expand paths.
    Table table = analysis.getParentTable().get(node);
    AbstractSqrlTable tbl = tables.get(table.getId());
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
    AbstractSqrlTable tbl = tables.get(ta.getId());
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


        AbstractSqrlTable table = this.tables.get(v.getId());
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
      Field field = analysis.getProducedField().get(node);

      SqlCall call = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{sqlNode,
          new SqlIdentifier(fieldNames.get(field), SqlParserPos.ZERO)}, SqlParserPos.ZERO);

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null,
          new SqlNodeList(List.of(call), SqlParserPos.ZERO), new SqlBasicCall(SqrlOperatorTable.AS,
          new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
              new SqlIdentifier(v.getId().getCanonical(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
              new SqlIdentifier("_", SqlParserPos.ZERO)}, SqlParserPos.ZERO), null, null, null,
          null, null, null, null, SqlNodeList.EMPTY);
      RelNode relNode = plan(select);
      this.fieldNames.put(field, relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount()-1).getName());
      AbstractSqrlTable table = this.tables.get(v.getId());
      RelDataTypeField produced = relNode.getRowType().getFieldList().get(0);
      RelDataTypeField newExpr = new RelDataTypeFieldImpl(produced.getName(),
          table.getRowType(null).getFieldList().size(), produced.getType());

      table.addField(newExpr);

      return select.getSelectList().get(0); //fully validated node
    }
  }

  @Override
  public SqlNode visitQueryAssignment(QueryAssignment node, Scope context) {
    Table ta = analysis.getParentTable().get(node);
    Optional<AbstractSqrlTable> tbl = (ta == null) ? Optional.empty() :
        Optional.ofNullable(tables.get(ta.getId()));

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
      AbstractSqrlTable t = this.tables.get(table.getId());
      RelDataTypeField field = relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount() - 1);
      RelDataTypeField newField = new RelDataTypeFieldImpl(
          node.getNamePath().getLast().getCanonical(), t.getRowType(null).getFieldCount(),
          field.getValue());

      t.addField(newField);

      return sqlNode;
    } else {
      this.tables.put(table.getId(), new QueryCalciteTable(relNode));
    }

    Relationship rel = (Relationship) analysis.getProducedField().get(node);
    if (rel == null) return null;
    //todo fix me
    Preconditions.checkNotNull(rel);
    this.getJoins().put(rel, new SqlJoinDeclaration(new SqlBasicCall(SqrlOperatorTable.AS,
        new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
            new SqlIdentifier(table.getId().getCanonical(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
            //todo fix
            new SqlIdentifier("_internal$1", SqlParserPos.ZERO)}, SqlParserPos.ZERO),
        Optional.empty()));

    if (table.getField(ReservedName.PARENT).isPresent()) {
      Relationship relationship = (Relationship) table.getField(ReservedName.PARENT).get();
      this.getJoins().put(relationship, new SqlJoinDeclaration(
          new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
              new SqlIdentifier(relationship.getToTable().getId().getCanonical(),
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
  public org.apache.calcite.schema.Table getTable(Name sqrlTableName) {
    org.apache.calcite.schema.Table table = this.tables.get(sqrlTableName);
    if (table != null) {
      return table;
    }
    throw new RuntimeException("Could not find table " + sqrlTableName);
  }
}
