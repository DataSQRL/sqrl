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
import ai.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.table.AddedColumn.Complex;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.Errors;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.generate.Generator.Scope;
import ai.datasqrl.plan.local.transpile.*;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
public class Generator extends AstVisitor<Void, Scope> implements SqrlCalciteBridge {

  final FieldNames fieldNames = new FieldNames();
  private final CalciteSchema sqrlSchema;
  private final CalciteSchema relSchema;
  ErrorCollector errors;
  CalciteTableFactory tableFactory;
  SchemaAdjustmentSettings schemaSettings;
  Planner planner;
  ImportManager importManager;
  UniqueAliasGenerator uniqueAliasGenerator;
  JoinDeclarationContainer joinDecs;
  SqlNodeBuilder sqlNodeBuilder;
  TableMapper tableMapper;
  VariableFactory variableFactory;
  JoinDeclarationFactory joinDeclarationFactory;

  public Generator(CalciteTableFactory tableFactory, SchemaAdjustmentSettings schemaSettings,
      Planner planner, ImportManager importManager, UniqueAliasGenerator uniqueAliasGenerator,
      JoinDeclarationContainer joinDecs, SqlNodeBuilder sqlNodeBuilder, TableMapper tableMapper,
      ErrorCollector errors, VariableFactory variableFactory) {
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

    joinDeclarationFactory = new JoinDeclarationFactory(tableMapper,
        uniqueAliasGenerator, new RexBuilder(planner.getTypeFactory()), this.fieldNames);

    //Time functions as a library poc
    sqrlSchema.add("time", new StdTimeLibrary());
    relSchema.add("time", new StdTimeLibrary());
  }

  public void generate(ScriptNode scriptNode) {
    for (Node statement : scriptNode.getStatements()) {
      statement.accept(this, new Scope());
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
   * {@link ai.datasqrl.plan.calcite.rules.SqrlExpansionRelRule}
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
      TableImport tblImport = importManager.importTable(sourceDataset, sourceTable, schemaSettings,
          errors);
      if (!tblImport.isSource()) {
        throw new RuntimeException("TBD");
      }
      SourceTableImport sourceTableImport = (SourceTableImport) tblImport;

      ScriptTableDefinition importedTable = tableFactory.importTable(sourceTableImport, nameAlias, planner.getRelBuilder());
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
      TranspiledResult result = transpile(query, Optional.empty(),
          TranspileOptions.builder().build());
      System.out.println(result.getRelNode().explain());
    }

    return null;
  }

  private void registerScriptTable(ScriptTableDefinition tblDef) {
    //Update table mapping from SQRL table to Calcite table...
    tblDef.getShredTableMap().values().stream().forEach(vt -> relSchema.add(vt.getNameId(), vt));
    QueryRelationalTable baseTable = tblDef.getBaseTable();
    relSchema.add(baseTable.getNameId(), baseTable);
    if (baseTable instanceof ProxyImportRelationalTable) {
      ImportedSourceTable sourceTable = ((ProxyImportRelationalTable)baseTable).getSourceTable();
      relSchema.add(sourceTable.getNameId(),sourceTable);
    }
    this.tableMapper.getTableMap().putAll(tblDef.getShredTableMap());
    //and also map all fields
    this.fieldNames.putAll(tblDef.getFieldNameMap());

    //Add all join declarations
    tblDef.getShredTableMap().keySet().stream().flatMap(t -> t.getAllRelationships()).forEach(r -> {
      SqlJoinDeclaration dec = joinDeclarationFactory.createParentChildJoinDeclaration(r);
      joinDecs.add(r, dec);
    });
    if (tblDef.getTable().getPath().size() == 1) {
      sqrlSchema.add(tblDef.getTable().getName().getDisplay(), (Table) tblDef.getTable());
    }
  }

  @SneakyThrows
  @Override
  public Void visitDistinctAssignment(DistinctAssignment node, Scope context) {
    TranspiledResult result = transpile(node.getSqlQuery(), Optional.empty(),
        TranspileOptions.builder().build());
    System.out.println(result.relNode.explain());
//    mapping.addQuery(relNode, context.getTable(), node.getNamePath());
    return null;
  }


  @SneakyThrows
  @Override
  public Void visitJoinAssignment(JoinAssignment node, Scope context) {
//    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_TABLE);
    Check.state(node.getNamePath().size() > 1, node, Errors.JOIN_ON_ROOT);
    Optional<SQRLTable> table = getContext(node.getNamePath().popLast());
    Preconditions.checkState(table.isPresent());
    String query = "SELECT * FROM _ " + node.getQuery();
    TableWithPK pkTable = tableMapper.getTable(table.get());

    TranspiledResult result = transpile(query, table,
        TranspileOptions.builder().orderToOrdinals(false).build());

    SqlJoinDeclaration joinDeclaration = joinDeclarationFactory.create(pkTable, result);
    VirtualRelationalTable vt = joinDeclarationFactory.getToTable(result.getSqlValidator(),
        result.getSqlNode());
    Multiplicity multiplicity = joinDeclarationFactory.deriveMultiplicity(result.getRelNode());
    SQRLTable toTable = tableMapper.getScriptTable(vt);
    Relationship relationship = variableFactory.addJoinDeclaration(node.getNamePath(), table.get(),
        toTable, multiplicity);

    //todo: assert non-shadowed
    this.fieldNames.put(relationship, relationship.getName().getCanonical());
    this.joinDecs.add(relationship, joinDeclaration);
    return null;
  }

  private Optional<SQRLTable> getContext(NamePath namePath) {
    if (namePath.size() == 0) {
      return Optional.empty();
    }
    SQRLTable table = (SQRLTable) sqrlSchema.getTable(namePath.get(0).getDisplay(), false)
        .getTable();

    return table.walkTable(namePath.popFirst());
  }

  @SneakyThrows
  private TranspiledResult transpile(String query, Optional<SQRLTable> context,
      TranspileOptions options) {
    SqlNode node = SqlParser.create(query,
            SqlParser.config().withCaseSensitive(false).withUnquotedCasing(Casing.UNCHANGED))
        .parseQuery();

    SqrlValidateResult result = validate(node, context);
    TranspiledResult transpiledResult = transpile(result.getValidated(), result.getValidator(),
        options);
    return transpiledResult;
  }

  private TranspiledResult transpile(SqlNode node, SqrlValidatorImpl validator,
      TranspileOptions options) {
    SqlSelect select =
        node instanceof SqlSelect ? (SqlSelect) node : (SqlSelect) ((SqlOrderBy) node).query;
    SqlValidatorScope scope = validator.getSelectScope(select);

    Transpile transpile = new Transpile(validator, tableMapper, uniqueAliasGenerator, joinDecs,
        sqlNodeBuilder, () -> new JoinBuilderImpl(uniqueAliasGenerator, joinDecs, tableMapper),
        fieldNames, options);

    transpile.rewriteQuery(select, scope);

    SqlValidator sqlValidator = TranspilerFactory.createSqlValidator(relSchema);
    SqlNode validated = sqlValidator.validate(select);

    return new TranspiledResult(select, validator, node, sqlValidator,
        plan(validated, sqlValidator));
  }

  private SqrlValidateResult validate(SqlNode node, Optional<SQRLTable> context) {
    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(sqrlSchema);
    sqrlValidator.setContext(context);
    sqrlValidator.validate(node);
    return new SqrlValidateResult(node, sqrlValidator);
  }

  @Override
  public Void visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    Optional<SQRLTable> table = getContext(node.getNamePath().popLast());
    Preconditions.checkState(table.isPresent());
    TranspiledResult result = transpile("SELECT " + node.getSql() + " FROM _", table,
        TranspileOptions.builder().build());

    return null;
  }

  @Override
  public Void visitQueryAssignment(QueryAssignment node, Scope context) {
    Optional<SQRLTable> ctx = getContext(node.getNamePath().popLast());
    TranspiledResult result = transpile(node.getSql(), ctx, TranspileOptions.builder().build());
    RelNode relNode = result.getRelNode();
    System.out.println(result.relNode.explain());
//    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_QUERY_TABLE);

    NamePath namePath = node.getNamePath();

    boolean isExpression = false;

    if (isExpression) {
      Check.state(node.getNamePath().size() > 1, node, Errors.QUERY_EXPRESSION_ON_ROOT);
      SQRLTable table = ctx.get();
      Column column = variableFactory.addQueryExpression(namePath, table);
      VirtualRelationalTable t = (VirtualRelationalTable) relSchema.getTable(
          this.tableMapper.getTable(table).getNameId(), false).getTable();
      RelDataTypeField field = relNode.getRowType().getFieldList()
          .get(relNode.getRowType().getFieldCount() - 1);
      RelDataTypeField newField = new RelDataTypeFieldImpl(
          node.getNamePath().getLast().getCanonical(), t.getRowType(null).getFieldCount(),
          field.getValue());

      this.fieldNames.put(column, newField.getName());

      AddedColumn addedColumn = new Complex(newField.getName(), result.getRelNode());
      t.addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem()));

    } else {
      List<Name> fieldNames = relNode.getRowType().getFieldList().stream()
          .map(f -> Name.system(f.getName())).collect(Collectors.toList());
      SQRLLogicalPlanConverter.ProcessedRel processedRel = optimize(relNode);
      ScriptTableDefinition queryTable = tableFactory.defineTable(namePath, processedRel,
          fieldNames);
      registerScriptTable(queryTable);
      Optional<Pair<Relationship, Relationship>> childRel = variableFactory.linkParentChild(
          namePath,
          queryTable.getTable(), ctx);

      childRel.ifPresent(rel -> {
        joinDecs.add(rel.getLeft(), joinDeclarationFactory.createChild(rel.getLeft()));
        joinDecs.add(rel.getRight(), joinDeclarationFactory.createParent(rel.getRight()));
      });
    }

    return null;
  }

  public SQRLLogicalPlanConverter.ProcessedRel optimize(RelNode relNode) {
    System.out.println("LP$0: \n" + relNode.explain());

    //Step 1: Push filters into joins so we can correctly identify self-joins
    relNode = planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN, relNode);
    System.out.println("LP$1: \n" + relNode.explain());

    //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
    //self-joins (including nested self-joins) as well as infer primary keys,
    //table types, and timestamps in the process

    SQRLLogicalPlanConverter sqrl2sql = new SQRLLogicalPlanConverter(
        () -> planner.getRelBuilder(), new SqrlRexUtil(planner.getRelBuilder().getRexBuilder()));
    relNode = relNode.accept(sqrl2sql);
    System.out.println("LP$2: \n" + relNode.explain());
    SQRLLogicalPlanConverter.ProcessedRel prel = sqrl2sql.postProcess(sqrl2sql.getRelHolder(relNode));
    System.out.println("LP$3: \n" + prel.getRelNode().explain());
    return prel;
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

  @Override
  public Table getTable(String tableName) {
    return relSchema.getTable(tableName, false).getTable();
  }

  public OptimizedDAG planDAG() {
    DAGPlanner dagPlanner = new DAGPlanner();
    //For now, we are going to generate simple scan queries for each virtual table
    //TODO: this needs to be replaced by the queries generated from the GraphQL API
    List<APIQuery> apiQueries = CalciteUtil.getTables(relSchema,VirtualRelationalTable.class).stream()
            .map(vt -> new APIQuery("query_"+vt.getNameId(),planner.getRelBuilder().scan(vt.getNameId()).build()))
            .collect(Collectors.toList());
    return dagPlanner.plan(relSchema, planner, apiQueries);
  }

  @Value
  class SqrlValidateResult {

    SqlNode validated;
    SqrlValidatorImpl validator;
  }

  @Value
  public class TranspiledResult {

    SqlNode sqrlNode;
    SqrlValidatorImpl sqrlValidator;
    SqlNode sqlNode;
    SqlValidator sqlValidator;
    RelNode relNode;
  }

  @Value
  class Scope {

  }
}
