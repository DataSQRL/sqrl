package ai.datasqrl.plan.local.generate;

import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.*;
import ai.datasqrl.plan.calcite.sqrl.rules.Sqrl2SqlLogicalPlanConverter;
import ai.datasqrl.plan.calcite.sqrl.table.*;
import ai.datasqrl.plan.calcite.sqrl.table.AddedColumn.Complex;
import ai.datasqrl.plan.calcite.sqrl.table.AddedColumn.Simple;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import ai.datasqrl.plan.local.ImportedTable;
import ai.datasqrl.plan.local.analyze.Analysis;
import ai.datasqrl.plan.local.generate.node.SqlJoinDeclaration;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.ScriptTable;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Generator extends QueryGenerator implements SqrlCalciteBridge {

  Planner planner;
  protected final CalciteTableFactory tableFactory;

  public Generator(Planner planner, CalciteTableFactory tableFactory, Analysis analysis) {
    super(analysis);
    this.planner = planner;
    this.tableFactory = tableFactory;
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
    List<ImportedTable> dt = analysis.getImportDataset().get(node);

    for (ImportedTable d : dt) {
      //Add all Calcite tables to the schema
      this.tables.put(d.getImpTable().getNameId(), d.getImpTable());
      d.getShredTableMap().values().stream().forEach(vt -> this.tables.put(vt.getNameId(),vt));

      //Update table mapping from SQRL table to Calcite table...
      this.tableMap.putAll(d.getShredTableMap());
      //and also map all fields
      this.fieldNames.putAll(d.getFieldNameMap());

      d.getShredTableMap().keySet().stream().flatMap(t->t.getAllRelationships())
          .forEach(this::createParentChildJoinDeclaration);
    }

    return null;
  }

  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignment node, Scope context) {
    SqlNode sqlNode = generateDistinctQuery(node);
    //Field names are the same...
    ScriptTable table = analysis.getProducedTable().get(node);

    RelNode relNode = plan(sqlNode);
    for (int i = 0; i < analysis.getProducedFieldList().get(node).size(); i++) {
      this.fieldNames.put(analysis.getProducedFieldList().get(node).get(i),
          relNode.getRowType().getFieldList().get(i).getName());
    }

    int numPKs = node.getPartitionKeyNodes().size();
    QueryCalciteTable queryTable = new QueryCalciteTable(relNode);
    this.tables.put(queryTable.getNameId(), queryTable);
    this.tableMap.put(table, queryTable);
    return sqlNode;
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignment node, Scope context) {
    //Create join declaration, recursively expand paths.
    ScriptTable table = analysis.getParentTable().get(node);
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
    ScriptTable v = analysis.getProducedTable().get(node);
    ScriptTable ta = analysis.getParentTable().get(node);
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

        AddedColumn addedColumn = new Complex(newExpr.getName(), relNode);
        ((VirtualSqrlTable)table).addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem()));

        return ctx.getAddlJoins().get(0).getRel();
      } else {
        throw new RuntimeException("TBD");
      }
    } else {
      //Simple
      AbstractSqrlTable t = tableMap.get(v);
      Preconditions.checkNotNull(t, "Could not find table", v);
      Field field = analysis.getProducedField().get(node);

      SqlCall call = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{sqlNode,
          new SqlIdentifier(getUniqueName(t, node.getNamePath().getLast().getCanonical()), SqlParserPos.ZERO)}, SqlParserPos.ZERO);

      SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null,
          new SqlNodeList(List.of(call), SqlParserPos.ZERO), new SqlBasicCall(SqrlOperatorTable.AS,
          new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
              new SqlIdentifier(tableMap.get(v).getNameId(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
              new SqlIdentifier("_", SqlParserPos.ZERO)}, SqlParserPos.ZERO), null, null, null,
          null, null, null, null, SqlNodeList.EMPTY);

      RelNode relNode = plan(select);
      this.fieldNames.put(field, relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount()-1).getName());
      VirtualSqrlTable table = (VirtualSqrlTable)this.tableMap.get(v);
      RelDataTypeField produced = relNode.getRowType().getFieldList().get(0);
      RelDataTypeField newExpr = new RelDataTypeFieldImpl(produced.getName(),
          table.getRowType(null).getFieldList().size(), produced.getType());

      RexNode rexNode = ((LogicalProject)relNode).getProjects().get(0);
      AddedColumn addedColumn = new Simple(newExpr.getName(), rexNode, false);
      table.addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem()));

      return select.getSelectList().get(0); //fully validated node
    }
  }

  private String getUniqueName(AbstractSqrlTable t, String newName) {
    List<String> toUnique = new ArrayList<>(t.getRowType().getFieldNames());
    toUnique.add(newName);
    List<String> uniqued = SqlValidatorUtil.uniquify(toUnique, false);
    return uniqued.get(uniqued.size()-1);
  }

  @Override
  public SqlNode visitQueryAssignment(QueryAssignment node, Scope context) {
    ScriptTable ta = analysis.getParentTable().get(node);
    Optional<AbstractSqrlTable> tbl = (ta == null) ? Optional.empty() :
        Optional.ofNullable(tableMap.get(ta));

    Scope scope = new Scope(tbl, true);
    SqlNode sqlNode = node.getQuery().accept(this, scope);
    RelNode relNode = plan(sqlNode);

    ScriptTable table = analysis.getProducedTable().get(node);

    if (analysis.getExpressionStatements().contains(node)) {
      Preconditions.checkNotNull(table);
      AbstractSqrlTable t = this.tableMap.get(table);
      RelDataTypeField field = relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount() - 1);
      RelDataTypeField newField = new RelDataTypeFieldImpl(
          node.getNamePath().getLast().getCanonical(), t.getRowType(null).getFieldCount(),
          field.getValue());
      this.fieldNames.put(analysis.getProducedField().get(node),
          newField.getName());
//      for (int i = 0; i < relNode.getRowType().getFieldCount() - scope.getPPKOffset(); i++) {
//        this.fieldNames.put(analysis.getProducedFieldList().get(node).get(i),
//            relNode.getRowType().getFieldList().get(i + scope.getPPKOffset()).getName());
//      }
      AddedColumn addedColumn = new Complex(newField.getName(), relNode);
      ((VirtualSqrlTable)t).addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem()));

      return sqlNode;
    } else {
      List<Field> tableFields = analysis.getProducedFieldList().get(node); //Fields that the user explicitly defined
      Sqrl2SqlLogicalPlanConverter.ProcessedRel processedRel = optimize(relNode);
      List<RelDataTypeField> relFields = processedRel.getRelNode().getRowType().getFieldList();

      for (int i = 0; i < tableFields.size(); i++) {
        this.fieldNames.put(tableFields.get(i), relFields.get(processedRel.getIndexMap().map(i)).getName());
      }

      QuerySqrlTable queryTable = tableFactory.getQueryTable(table.getName(), processedRel);
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
//      this.fieldNames.put(relationship, ReservedName.PARENT.getCanonical());
      this.getJoins().put(relationship, new SqlJoinDeclaration(
          new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
              new SqlIdentifier(tableMap.get(relationship.getToTable()).getNameId(),
                  SqlParserPos.ZERO), SqlNodeList.EMPTY),
              new SqlIdentifier(ReservedName.PARENT.getCanonical(), SqlParserPos.ZERO)},
              SqlParserPos.ZERO), Optional.empty()));
    }

    //add parent relationsihp

    return null;
  }

  public Sqrl2SqlLogicalPlanConverter.ProcessedRel optimize(RelNode relNode) {
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
    return sqrl2sql.putPrimaryKeysUpfront(sqrl2sql.getRelHolder(relNode));
  }

  @SneakyThrows
  private RelNode plan(SqlNode sqlNode) {
    System.out.println(sqlNode);
    planner.refresh();
    try {
      planner.validate(sqlNode);
    } catch (Exception e) {
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
