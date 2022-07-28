package ai.datasqrl.plan.local.generate;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.environment.ImportManager.TableImport;
import ai.datasqrl.function.builtin.time.StdTimeLibrary;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.*;
import ai.datasqrl.plan.calcite.sqrl.rules.Sqrl2SqlLogicalPlanConverter;
import ai.datasqrl.plan.calcite.sqrl.table.*;
import ai.datasqrl.plan.calcite.sqrl.table.AddedColumn.Complex;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import ai.datasqrl.plan.local.Errors;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.analyze.VariableFactory;
import ai.datasqrl.plan.local.generate.Generator.Scope;
import ai.datasqrl.plan.local.generate.QueryGenerator.FieldNames;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.ScriptTable;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.JoinDeclaration;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;

import java.util.*;
import java.util.stream.Collectors;

import static ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil.and;

@Getter
public class Generator extends DefaultTraversalVisitor<Void, Scope> implements SqrlCalciteBridge {

  protected final Map<ScriptTable, AbstractSqrlTable> tableMap = new HashMap<>();
  final FieldNames fieldNames = new FieldNames();
  private final CalciteSchema sqrlSchema;
  private final CalciteSchema relSchema;
  ErrorCollector errors;
  CalciteTableFactory tableFactory;
  SchemaAdjustmentSettings schemaSettings;
  Planner planner;

  ImportManager importManager;
  UniqueAliasGeneratorImpl uniqueAliasGenerator;
  JoinDeclarationContainerImpl joinDecs;
  SqlNodeBuilderImpl sqlNodeBuilder;
  TableMapperImpl tableMapper;
  VariableFactory variableFactory;


  public Generator(CalciteTableFactory tableFactory, SchemaAdjustmentSettings schemaSettings,
      Planner planner, ImportManager importManager, UniqueAliasGeneratorImpl uniqueAliasGenerator,
      JoinDeclarationContainerImpl joinDecs, SqlNodeBuilderImpl sqlNodeBuilder,
      TableMapperImpl tableMapper, ErrorCollector errors, VariableFactory variableFactory) {
    this.tableFactory = tableFactory;
    this.schemaSettings = schemaSettings;
    this.planner = planner;
    this.importManager = importManager;
    this.uniqueAliasGenerator = uniqueAliasGenerator;
    this.joinDecs = joinDecs;
    this.sqlNodeBuilder = sqlNodeBuilder;
    this.tableMapper = tableMapper;
    this.errors = errors;
    this.variableFactory = variableFactory;
    this.relSchema = planner.getDefaultSchema().unwrap(CalciteSchema.class);
    this.sqrlSchema = CalciteSchema.createRootSchema(true);

    //Time functions as a library poc
    sqrlSchema.add("time", new StdTimeLibrary());
    relSchema.add("time", new StdTimeLibrary());
  }

  public void generate(ScriptNode scriptNode) {
    for (Node statement : scriptNode.getStatements()) {
      Optional<ScriptTable> context =
          (statement instanceof Assignment) ? getContext(((Assignment) statement).getNamePath())
              : Optional.empty();
      statement.accept(this, new Scope(context));
    }
  }

  public Void generate(SqrlStatement statement) {
    return statement.accept(this, null);
  }

  /**
   * In order to expose a hierarchical table in Calcite, we need to register the source dataset with
   * the full calcite schema and a table for each nested record, which we can then query.
   * <p>
   * To do this, we use a process called table shredding. This involves removing the nested records
   * from the source dataset, and then registering the resulting table with the calcite schema.
   * <p>
   * We can then expand this into a full logical plan using the
   * {@link ai.datasqrl.plan.calcite.sqrl.rules.SqrlExpansionRelRule}
   */
  @Override
  public Void visitImportDefinition(ImportDefinition node, Scope context) {
    if (node.getNamePath().size() != 2) {
      throw new RuntimeException(
          String.format("Invalid import identifier: %s", node.getNamePath()));
    }

    //Check if this imports all or a single table
    Name sourceDataset = node.getNamePath().get(0);
    Name sourceTable = node.getNamePath().get(1);

    List<TableImport> importTables;
    Optional<Name> nameAlias = node.getAliasName();
    List<ScriptTableDefinition> dt = new ArrayList<>();
    if (sourceTable.equals(ReservedName.ALL)) { //import all tables from dataset
      importTables = importManager.importAllTables(sourceDataset, schemaSettings, errors);
      nameAlias = Optional.empty();
      throw new RuntimeException("TBD");
    } else { //importing a single table
      //todo: Check if table exists
      TableImport tblImport = importManager.importTable(sourceDataset, sourceTable,
          schemaSettings, errors);
      if (!tblImport.isSource()) {
        throw new RuntimeException("TBD");
      }
      SourceTableImport sourceTableImport = (SourceTableImport)tblImport;

      ScriptTableDefinition importedTable = tableFactory.importTable(sourceTableImport, nameAlias);
      dt.add(importedTable);
    }

    for (ScriptTableDefinition d : dt) {
      registerScriptTable(d);
    }

    Check.state(!(node.getTimestamp().isPresent() && sourceTable.equals(ReservedName.ALL)), node,
        Errors.TIMESTAMP_NOT_ALLOWED);
    if (node.getTimestamp().isPresent()) {
      String query = String.format("SELECT %s FROM %s",
          NodeFormatter.accept(node.getTimestamp().get()),
          dt.get(0).getTable().getName().getDisplay());
      TranspiledResult result = transpile(query, Optional.empty());
      System.out.println(result.getRelNode().explain());
    }

    return null;
  }

  private void registerScriptTable(ScriptTableDefinition tblDef) {
    //Update table mapping from SQRL table to Calcite table...
    tblDef.getShredTableMap().values().stream().forEach(vt -> relSchema.add(vt.getNameId(),vt));
    relSchema.add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
    this.tableMapper.getTableMap().putAll(tblDef.getShredTableMap());
    //and also map all fields
    this.fieldNames.putAll(tblDef.getFieldNameMap());

    //Add all join declarations
    tblDef.getShredTableMap().keySet().stream().flatMap(t->t.getAllRelationships())
            .forEach(r->{
              JoinDeclaration dec = createParentChildJoinDeclaration(r, tableMapper, uniqueAliasGenerator);
              joinDecs.add(r, dec);
            });
    if (tblDef.getTable().getPath().size() == 1) {
      sqrlSchema.add(tblDef.getTable().getName().getDisplay(),
              (Table) tblDef.getTable());
    }
  }

  @SneakyThrows
  @Override
  public Void visitDistinctAssignment(DistinctAssignment node, Scope context) {
    TranspiledResult result = transpile(node.getSqlQuery(), Optional.empty());
    System.out.println(result.relNode.explain());
//    mapping.addQuery(relNode, context.getTable(), node.getNamePath());
    return null;
  }


  @SneakyThrows
  @Override
  public Void visitJoinAssignment(JoinAssignment node, Scope context) {
//    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_TABLE);
    Check.state(node.getNamePath().size() > 1, node, Errors.JOIN_ON_ROOT);
    Optional<ScriptTable> table = getContext(node.getNamePath().popLast());
    Preconditions.checkState(table.isPresent());
    String query = "SELECT * FROM _ " + node.getQuery();
    TranspiledResult result = transpile(query, table);

    SqlBasicCall tRight = (SqlBasicCall)getRightDeepTable(result.getSqlNode());
    VirtualSqrlTable vt = result.getSqlValidator().getNamespace(tRight).getTable().unwrap(VirtualSqrlTable.class);
    ScriptTable toTable = getScriptTable(vt);

    Relationship relationship = variableFactory.addJoinDeclaration(node.getNamePath(), table.get(),
        toTable, node.getJoinDeclaration().getLimit());

    //todo: assert non-shadowed
    this.fieldNames.put(relationship, relationship.getName().getCanonical());
    SqlNode join = unwrapSelect(result.getSqlNode()).getFrom();

    SqlJoin join1 = (SqlJoin) join;

    this.joinDecs.add(relationship, new JoinDeclarationImpl(
        Optional.of(join1.getCondition()),
        join1.getRight(),
        "_",
        ((SqlIdentifier) tRight.getOperandList().get(1)).names.get(0)));
    return null;
  }

  private ScriptTable getScriptTable(VirtualSqrlTable vt) {
    for (Map.Entry<ScriptTable, AbstractSqrlTable> t : this.tableMapper.getTableMap().entrySet()) {
      if (t.getValue().equals(vt)) {
        return t.getKey();
      }
    }
    return null;
  }

  private SqlNode getRightDeepTable(SqlNode node) {
    if (node instanceof SqlSelect) {
      return getRightDeepTable(((SqlSelect) node).getFrom());
    } else if (node instanceof SqlOrderBy) {
      return getRightDeepTable(((SqlOrderBy) node).query);
    } else if (node instanceof SqlJoin) {
      return getRightDeepTable(((SqlJoin) node).getRight());
    } else {
      return node;
    }
  }

  //todo: fix for union etc
  private SqlSelect unwrapSelect(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOrderBy) {
      return (SqlSelect) ((SqlOrderBy) sqlNode).query;
    }
    return (SqlSelect) sqlNode;
  }

  private ScriptTable getContextOrThrow(NamePath namePath) {
    return getContext(namePath).orElseThrow(() -> new RuntimeException(""));
  }

  private Optional<ScriptTable> getContext(NamePath namePath) {
    if (namePath.size() == 0) {
      return Optional.empty();
    }
    ScriptTable table = (ScriptTable) sqrlSchema.getTable(namePath.get(0).getDisplay(), false)
        .getTable();

    return table.walkTable(namePath.popFirst());
  }

  @SneakyThrows
  private TranspiledResult transpile(String query, Optional<ScriptTable> context) {
    System.out.println(query);
    SqlNode node = SqlParser.create(query, SqlParser.config().withCaseSensitive(false)
        .withUnquotedCasing(Casing.UNCHANGED)).parseQuery();

    SqrlValidateResult result = validate(node, context);
    TranspiledResult transpiledResult = transpile(result.getValidated(), result.getValidator());
    return transpiledResult;
  }

  private TranspiledResult transpile(SqlNode node, SqrlValidatorImpl validator) {
    SqlSelect select =
        node instanceof SqlSelect ? (SqlSelect) node : (SqlSelect) ((SqlOrderBy) node).query;
    SqlValidatorScope scope = validator.getSelectScope(select);

    Transpile transpile = new Transpile(
        validator, tableMapper, uniqueAliasGenerator, joinDecs,
        sqlNodeBuilder,
        () -> new JoinBuilder(uniqueAliasGenerator, joinDecs, tableMapper, sqlNodeBuilder),
        fieldNames);
    System.out.println("Original: " + select);

    transpile.rewriteQuery(select, scope);

    System.out.println("Rewritten: " + select);
    SqlValidator sqlValidator = TranspilerFactory.createSqlValidator(
        relSchema);
    SqlNode validated = sqlValidator.validate(select);

    return new TranspiledResult(select, validator, node,
        sqlValidator, plan(validated, sqlValidator));
  }

  private SqrlValidateResult validate(SqlNode node, Optional<ScriptTable> context) {
    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(sqrlSchema);
    sqrlValidator.setContext(context);
    sqrlValidator.validate(node);
    return new SqrlValidateResult(node, sqrlValidator);
  }

  @Override
  public Void visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    Optional<ScriptTable> table = getContext(node.getNamePath().popLast());
    Preconditions.checkState(table.isPresent());
    TranspiledResult result = transpile("SELECT " + node.getSql() + " FROM _", table);

    System.out.println(result);

//
//
//    ScriptTable v = analysis.getProducedTable().get(node);
//    ScriptTable ta = analysis.getParentTable().get(node);
//    AbstractSqrlTable tbl = tableMap.get(ta);
//    Scope ctx = new Scope(Optional.ofNullable(tbl), true);
//    SqlNode sqlNode = node.getExpression().accept(this, ctx);
//
//    if (!ctx.getAddlJoins().isEmpty()) {
//      //With subqueries
//      if (ctx.getAddlJoins().size() > 1) {
//        throw new RuntimeException("TBD");
//      } else if (sqlNode instanceof SqlIdentifier) {
//        //Just one subquery and just a literal assignment. No need to rejoin to parent.
//        //e.g. Orders.total := sum(entries.total);
//        //AS
//        SqlBasicCall call = (SqlBasicCall) ctx.getAddlJoins().get(0).getRel();
//        RelNode relNode = plan(call.getOperandList().get(0));
//        Field field = analysis.getProducedField().get(node);
//        this.fieldNames.put(field, relNode.getRowType().getFieldList()
//            .get(relNode.getRowType().getFieldCount()-1).getName());
//
//
//        AbstractSqrlTable table = this.tableMap.get(v);
//        RelDataTypeField produced = relNode.getRowType().getFieldList()
//            .get(relNode.getRowType().getFieldList().size() - 1);
//        RelDataTypeField newExpr = new RelDataTypeFieldImpl(fieldNames.get(field),
//            table.getRowType(null).getFieldList().size(), produced.getType());
//
//        AddedColumn addedColumn = new Complex(newExpr.getName(), relNode);
//        ((VirtualSqrlTable)table).addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem
//        ()));
//
//        return ctx.getAddlJoins().get(0).getRel();
//      } else {
//        throw new RuntimeException("TBD");
//      }
//    } else {
//      //Simple
//      AbstractSqrlTable t = tableMap.get(v);
//      Preconditions.checkNotNull(t, "Could not find table", v);
//      Field field = analysis.getProducedField().get(node);
//
//      SqlCall call = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{sqlNode,
//          new SqlIdentifier(getUniqueName(t, node.getNamePath().getLast().getCanonical()),
//          SqlParserPos.ZERO)}, SqlParserPos.ZERO);
//
//      SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null,
//          new SqlNodeList(List.of(call), SqlParserPos.ZERO), new SqlBasicCall(SqrlOperatorTable
//          .AS,
//          new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
//              new SqlIdentifier(tableMap.get(v).getNameId(), SqlParserPos.ZERO), SqlNodeList
//              .EMPTY),
//              new SqlIdentifier("_", SqlParserPos.ZERO)}, SqlParserPos.ZERO), null, null, null,
//          null, null, null, null, SqlNodeList.EMPTY);
//
//      RelNode relNode = plan(select);
//      this.fieldNames.put(field, relNode.getRowType().getFieldList()
//          .get(relNode.getRowType().getFieldCount()-1).getName());
//      VirtualSqrlTable table = (VirtualSqrlTable)this.tableMap.get(v);
//      RelDataTypeField produced = relNode.getRowType().getFieldList().get(0);
//      RelDataTypeField newExpr = new RelDataTypeFieldImpl(produced.getName(),
//          table.getRowType(null).getFieldList().size(), produced.getType());
//
//      RexNode rexNode = ((LogicalProject)relNode).getProjects().get(0);
//      AddedColumn addedColumn = new Simple(newExpr.getName(), rexNode, false);
//      table.addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem()));
//
//      return select.getSelectList().get(0); //fully validated node
//    }
    return null;
  }

  private String getUniqueName(AbstractSqrlTable t, String newName) {
    List<String> toUnique = new ArrayList<>(t.getRowType().getFieldNames());
    toUnique.add(newName);
    List<String> uniqued = SqlValidatorUtil.uniquify(toUnique, false);
    return uniqued.get(uniqued.size() - 1);
  }

  @Override
  public Void visitQueryAssignment(QueryAssignment node, Scope context) {
    Optional<ScriptTable> ctx = getContext(node.getNamePath().popLast());
    TranspiledResult result = transpile(node.getSql(), ctx);
    RelNode relNode = result.getRelNode();
    System.out.println(result.relNode.explain());
//    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_QUERY_TABLE);

    NamePath namePath = node.getNamePath();

    boolean isExpression = false;

    if (isExpression) {
      Check.state(node.getNamePath().size() > 1, node, Errors.QUERY_EXPRESSION_ON_ROOT);
      ScriptTable table = ctx.get();
      Column column = variableFactory.addQueryExpression(namePath, table);
      VirtualSqrlTable t = (VirtualSqrlTable) relSchema.getTable(
          this.tableMapper.getTable(table).getNameId(), false).getTable();
      RelDataTypeField field = relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount() - 1);
      RelDataTypeField newField = new RelDataTypeFieldImpl(
          node.getNamePath().getLast().getCanonical(), t.getRowType(null).getFieldCount(),
          field.getValue());

      this.fieldNames.put(column,
          newField.getName());

      AddedColumn addedColumn = new Complex(newField.getName(), result.getRelNode());
      t.addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem()));

    } else {
      List<Name> fieldNames = relNode.getRowType().getFieldList().stream().map(f -> Name.system(f.getName())).collect(Collectors.toList());
      Sqrl2SqlLogicalPlanConverter.ProcessedRel processedRel = optimize(relNode);
      ScriptTableDefinition queryTable = tableFactory.defineTable(namePath, processedRel, fieldNames);
      registerScriptTable(queryTable);
      Optional<Relationship> childRel = variableFactory.linkParentChild(namePath,queryTable.getTable(),ctx);
      childRel.ifPresent(rel -> joinDecs.add(rel, new JoinDeclarationImpl(
              Optional.empty(),
              createParentChildCondition(rel, "x", this.tableMapper),
              "_",
              "x")));
    }

    return null;
  }

  public Sqrl2SqlLogicalPlanConverter.ProcessedRel optimize(RelNode relNode) {
    System.out.println("LP$0: \n" + relNode.explain());

    //Step 1: Push filters into joins so we can correctly identify self-joins
    relNode = planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN, relNode);
    System.out.println("LP$1: \n" + relNode.explain());

    //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
    //self-joins (including nested self-joins) as well as infer primary keys,
    //table types, and timestamps in the process

    Sqrl2SqlLogicalPlanConverter sqrl2sql = new Sqrl2SqlLogicalPlanConverter(
        () -> planner.getRelBuilder(),
        new SqrlRexUtil(planner.getRelBuilder().getRexBuilder()));
    relNode = relNode.accept(sqrl2sql);
    System.out.println("LP$2: \n" + relNode.explain());
    return sqrl2sql.putPrimaryKeysUpfront(sqrl2sql.getRelHolder(relNode));
  }

  @SneakyThrows
  private RelNode plan(SqlNode sqlNode, SqlValidator sqlValidator) {
    System.out.println(sqlNode);
    planner.refresh();
    planner.setValidator(sqlNode, sqlValidator);

    RelRoot root = planner.rel(sqlNode);
    RelNode relNode = root.rel;

    return relNode;
  }

  protected JoinDeclaration createParentChildJoinDeclaration(Relationship rel,
      TableMapperImpl tableMapper,
      UniqueAliasGeneratorImpl uniqueAliasGenerator) {
    TableWithPK pk = tableMapper.getTable(rel.getToTable());
    String alias = uniqueAliasGenerator.generate(pk);
    return new JoinDeclarationImpl(
        Optional.of(createParentChildCondition(rel, alias, tableMapper)),
        createTableRef(rel.getToTable(), alias, tableMapper), "_", alias);
  }

  protected SqlNode createTableRef(ScriptTable table, String alias, TableMapperImpl tableMapper) {
    return new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
        new SqlIdentifier(tableMapper.getTable(table).getNameId(), SqlParserPos.ZERO),
        SqlNodeList.EMPTY),
        new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  protected SqlNode createParentChildCondition(Relationship rel, String alias,
      TableMapperImpl tableMapper) {
    TableWithPK lhs =
        rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMapper.getTable(
            rel.getFromTable())
            : tableMapper.getTable(rel.getToTable());
    TableWithPK rhs =
        rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMapper.getTable(
            rel.getToTable())
            : tableMapper.getTable(rel.getFromTable());

    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < lhs.getPrimaryKeys().size(); i++) {
      String lpk = lhs.getPrimaryKeys().get(i);
      String rpk = rhs.getPrimaryKeys().get(i);
      conditions.add(new SqlBasicCall(SqrlOperatorTable.EQUALS,
          new SqlNode[]{new SqlIdentifier(List.of("_", lpk), SqlParserPos.ZERO),
              new SqlIdentifier(List.of(alias, rpk), SqlParserPos.ZERO)}, SqlParserPos.ZERO));
    }

    return and(conditions);
  }

  @Override
  public Table getTable(String tableName) {
    return relSchema.getTable(tableName, false).getTable();
  }

  @Value
  class SqrlValidateResult {

    SqlNode validated;
    SqrlValidatorImpl validator;
  }

  @Value
  class TranspiledResult {

    SqlNode sqrlNode;
    SqrlValidatorImpl sqrlValidator;
    SqlNode sqlNode;
    SqlValidator sqlValidator;
    RelNode relNode;
  }

  @Value
  class Scope {

    Optional<ScriptTable> table;
  }
}
