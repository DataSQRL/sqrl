package ai.datasqrl.plan.local.generate;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import ai.datasqrl.plan.calcite.TranspilerFactory;
import ai.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.table.AddedColumn.Complex;
import ai.datasqrl.plan.calcite.table.AddedColumn.Simple;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.transpile.*;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.datasqrl.plan.local.generate.Resolve.OpKind.IMPORT_TIMESTAMP;

@Getter
public class Resolve {

  private final CalciteTableFactory tableFactory = new CalciteTableFactory(
          new SqrlTypeFactory(new SqrlTypeSystem()));

  enum ImportState {
    UNRESOLVED, RESOLVING, RESOLVED;
  }

  @Getter
  public class Env {
    VariableFactory variableFactory = new VariableFactory();
    List<StatementOp> ops = new ArrayList<>();
    Map<Relationship, SqlJoinDeclaration> resolvedJoinDeclarations = new HashMap<>();
    List<SqrlStatement> queryOperations = new ArrayList<>();

    // Mappings
    Map<SQRLTable, VirtualRelationalTable> tableMap = new HashMap<>();
    Map<Field, String> fieldMap = new HashMap<>();
    UniqueAliasGenerator aliasGenerator = new UniqueAliasGeneratorImpl();

    SqrlCalciteSchema sqrlSchema;
    CalciteSchema relSchema;

    Session session;
    ScriptNode scriptNode;

    public Env(SqrlCalciteSchema sqrlSchema, CalciteSchema relSchema, Session session,
        ScriptNode scriptNode) {
      this.sqrlSchema = sqrlSchema;
      this.relSchema = relSchema;
      this.session = session;
      this.scriptNode = scriptNode;
    }
  }

  public Env planDag(Session session, ScriptNode script) {
    Env env = createEnv(session, script);
    resolveImports(env);
    mapOperations(env);
    planQueries(env);
    return env;
  }

  public Env createEnv(Session session, ScriptNode script) {
    // All operations are applied to this env
    return new Env(
        new SqrlCalciteSchema(CalciteSchema.createRootSchema(true).schema),
        session.planner.getDefaultSchema().unwrap(CalciteSchema.class),
        session,
        script
    );
  }

  private void validateImportInHeader(Env env) {
    boolean inHeader = true;
    for (SqlNode statement : env.scriptNode.getStatements()) {
      if (statement instanceof ImportDefinition) {
        Preconditions.checkState(inHeader, "Import statements must be in header");
      } else {
        inHeader = false;
      }
    }
  }

  void resolveImports(Env env) {
    validateImportInHeader(env);

    resolveImportDefinitions(env);
  }

  void resolveImportDefinitions(Env env) {
    for (SqlNode statement : env.scriptNode.getStatements()) {
      if (statement instanceof ImportDefinition) {
        resolveImportDefinition(env, (ImportDefinition) statement);
      }
    }
  }

  void mapOperations(Env env) {
    for (SqlNode statement : env.scriptNode.getStatements()) {
      env.queryOperations.add((SqrlStatement) statement);
    }
  }

  private void resolveImportDefinition(Env env, ImportDefinition node) {
    SourceTableImport tblImport = lookupDatasetModule(env, node.getImportPath());

    ScriptTableDefinition importedTable = createScriptTableDefinition(env, tblImport,
        node.getAlias());

    registerScriptTable(env, importedTable);
  }

  private SourceTableImport lookupDatasetModule(Env env, NamePath namePath) {
    Preconditions.checkState(namePath.size() == 2, "IMPORT malformed");
    return (SourceTableImport) env.session.importManager.importTable(namePath.get(0),
        namePath.get(1),
        SchemaAdjustmentSettings.DEFAULT, env.session.errors);
  }

  private ScriptTableDefinition createScriptTableDefinition(Env env, SourceTableImport tblImport,
      Optional<Name> alias) {
    return tableFactory.importTable(tblImport, alias,
        env.session.planner.getRelBuilder());
  }

  private void registerScriptTable(Env env, ScriptTableDefinition tblDef) {
    //Update table mapping from SQRL table to Calcite table...

    tblDef.getShredTableMap().values().stream().forEach(vt ->
        env.relSchema.add(vt.getNameId(), vt));
    env.relSchema.add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
    if (tblDef.getBaseTable() instanceof ProxyImportRelationalTable) {
      ImportedSourceTable impTable = ((ProxyImportRelationalTable) tblDef.getBaseTable()).getSourceTable();
      env.relSchema.add(impTable.getNameId(),impTable);
    }
    env.tableMap.putAll(tblDef.getShredTableMap());
    //and also map all fields
    env.fieldMap.putAll(tblDef.getFieldNameMap());

    JoinDeclarationFactory joinDeclarationFactory = new JoinDeclarationFactory(env);
    //Add all join declarations
    tblDef.getShredTableMap().keySet().stream().flatMap(t ->
        t.getAllRelationships()).forEach(r -> {
      SqlJoinDeclaration dec =
          joinDeclarationFactory.createParentChildJoinDeclaration(r);
      env.resolvedJoinDeclarations.put(r, dec);
    });
    if (tblDef.getTable().getPath().size() == 1) {
      env.sqrlSchema.add(tblDef.getTable().getName().getDisplay(), (org.apache.calcite.schema.Table)
          tblDef.getTable());
    }
  }

  void planQueries(Env env) {
    // Go through each operation and resolve
    for (SqrlStatement q : env.queryOperations) {
      planQuery(env, q);
      if (env.session.errors.hasErrors()) {
        return;
      }
    }
  }

  void planQuery(Env env, SqrlStatement statement) {
    createStatementOp(env, statement)
        .ifPresent(op -> planOp(env, op));
  }

  private void planOp(Env env, StatementOp op) {
    validate(env, op);
    transpile(env, op);
    computeOpKind(env, op);
    plan(env, op);
    applyOp(env, op);
  }

  private void computeOpKind(Env env, StatementOp op) {

    op.setKind(getKind(env, op));
    /**
     * if op is join:
     *  if context ->
     */
  }

  private OpKind getKind(Env env, StatementOp op) {
    if (op.statement instanceof JoinAssignment) {
      if (op.getStatement().getNamePath().size() == 1) {
        return OpKind.QUERY;
      } else {
        return OpKind.JOIN;
      }
    } else if (op.statement instanceof ExpressionAssignment) {
      return OpKind.EXPR;
    } else if (op.statement instanceof QueryAssignment
        || op.statement instanceof DistinctAssignment) {
      Optional<SQRLTable> ctx = getContext(env, (Assignment) op.statement);
      if (ctx.isEmpty()) {
        return OpKind.ROOT_QUERY;
      }

      return isExpressionQuery(op) ? OpKind.EXPR_QUERY : OpKind.QUERY;
    } else if (op.statement instanceof ImportDefinition) {
      return IMPORT_TIMESTAMP;
    }
    return null;
  }

  /**
   * Determining if a query is an expression or query follows these rules: 1. has a single column 2.
   * Is aggregating and is unnamed
   */
  private boolean isExpressionQuery(StatementOp op) {
    if (CalciteUtil.isSingleUnnamedColumn(op.getQuery()) &&
        CalciteUtil.isAggregating(op.getQuery(), op.getSqrlValidator())) {
      return true;
    }

    return false;
  }

  private Optional<StatementOp> createStatementOp(Env env, SqrlStatement statement) {
    SqlNode sqlNode = null;

    StatementKind statementKind;
    if (statement instanceof ExpressionAssignment) {
      sqlNode = ((ExpressionAssignment) statement).getExpression();
      statementKind = StatementKind.EXPR;
    } else if  (statement instanceof QueryAssignment) {
      sqlNode = ((QueryAssignment) statement).getQuery();
      statementKind = StatementKind.QUERY;
    } else if  (statement instanceof CreateSubscription) {
      sqlNode = ((CreateSubscription) statement).getQuery();
      statementKind = StatementKind.SUBSCRIPTION;
    } else if  (statement instanceof DistinctAssignment) {
      sqlNode = ((DistinctAssignment) statement).getQuery();
      statementKind = StatementKind.DISTINCT_ON;
    } else if  (statement instanceof JoinAssignment) {
      sqlNode = ((JoinAssignment) statement).getQuery();
      statementKind = StatementKind.JOIN;
    } else if (statement instanceof ImportDefinition) {
      ImportDefinition importDef = (ImportDefinition) statement;
      if (importDef.getTimestamp().isEmpty()) {
        return Optional.empty();
      }
      sqlNode = importDef.getTimestamp().get();
      statementKind = StatementKind.IMPORT;

    }
    else {
      throw new RuntimeException("Unrecognized assignment type");
    }

    StatementOp op = new StatementOp(statement, sqlNode, statementKind);
    env.ops.add(op);
    return Optional.of(op);
  }

  public void validate(Env env, StatementOp op) {
    // Validator validator = createValidator(op);
    // validator.validate(env, op);
  }

  public void transpile(Env env, StatementOp op) {
    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(env.sqrlSchema);
    SqlNode newNode = sqrlValidator.validate(op);
    op.setQuery(newNode);
    op.setSqrlValidator(sqrlValidator);

    Transpile transpile = new Transpile(env, op,
        TranspileOptions.builder().orderToOrdinals(true).build());
    SqlSelect select =
      op.query instanceof SqlSelect ? (SqlSelect) op.query : (SqlSelect) ((SqlOrderBy)
          op.query).query;
    transpile.rewriteQuery(select, sqrlValidator.getSelectScope(select));
    validateSql(env, op);
  }

  private Optional<SQRLTable> getContext(Env env, SqrlStatement statement) {
    if (statement.getNamePath().size() <= 1) {
      return Optional.empty();
    }
    SQRLTable table = (SQRLTable)
        env.sqrlSchema.getTable(statement.getNamePath().get(0).getDisplay(), false)
            .getTable();

    if (statement.getNamePath().popFirst().size() == 1) {
      return Optional.of(table);
    }

    return table.walkTable(statement.getNamePath().popFirst().popLast());
  }

  public void validateSql(Env env, StatementOp op) {
    SqlValidator sqlValidator = TranspilerFactory.createSqlValidator(env.relSchema);
    op.setSqlValidator(sqlValidator);

    SqlNode validated = sqlValidator.validate(op.query);
    op.setValidatedSql(validated);
  }

  public void plan(Env env, StatementOp op) {
    env.session.planner.refresh();
    env.session.planner.setValidator(op.validatedSql, op.sqlValidator);

    RelNode relNode = env.session.planner.rel(op.validatedSql).rel;

    op.setRelNode(relNode);
  }

  public SQRLLogicalPlanConverter.RelMeta optimize(Env env, StatementOp op) {
    List<String> fieldNames = op.relNode.getRowType().getFieldNames();
//    System.out.println("LP$0: \n" + op.relNode.explain());

    //Step 1: Push filters into joins so we can correctly identify self-joins
    RelNode relNode = env.session.planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN,
        op.relNode);
//    System.out.println("LP$1: \n" + relNode.explain());

    //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
    //self-joins (including nested self-joins) as well as infer primary keys,
    //table types, and timestamps in the process

    //TODO: extract materialization preference from hints if present
    SQRLLogicalPlanConverter sqrl2sql = new SQRLLogicalPlanConverter(getRelBuilderFactory(env), Optional.empty());
    relNode = relNode.accept(sqrl2sql);
//    System.out.println("LP$2: \n" + relNode.explain());
    SQRLLogicalPlanConverter.RelMeta prel = sqrl2sql.postProcess(sqrl2sql.getRelHolder(relNode),fieldNames);
//    System.out.println("LP$3: \n" + prel.getRelNode().explain());
    return prel;
  }

  private Supplier<RelBuilder> getRelBuilderFactory(Env env) {
    return () -> env.session.planner.getRelBuilder();
  }

  public void applyOp(Env env, StatementOp op) {
    switch (op.kind) {
      case EXPR:
      case EXPR_QUERY:
      case IMPORT_TIMESTAMP:
        AddedColumn c = createColumnAddOp(env, op);
        addColumn(env, op, c, op.kind==IMPORT_TIMESTAMP);
        break;
      case ROOT_QUERY:
        createTable(env, op);
        // env.relSchema.add(q.getName(), q);
//        updateTableMapping(env, op, q);
        break;
      case QUERY:
        createTable(env, op);

//        updateTableMapping(env, op, q2);
//        updateRelMapping(env, op, s, v);
        break;
      case JOIN:
        updateJoinMapping(env, op);
    }

//    env.fieldMap.putAll(op.getFieldMapping());
  }

  private void addColumn(Env env, StatementOp op, AddedColumn c, boolean fixTimestamp) {
    Optional<VirtualRelationalTable> optTable = getTargetRelTable(env, op);
    Check.state(optTable.isPresent(), null, null);

    VirtualRelationalTable vtable = optTable.get();
    Optional<Integer> timestampScore = tableFactory.getTimestampScore(op.statement.getNamePath().getLast(),c.getDataType());
    vtable.addColumn(c, tableFactory.getTypeFactory(), getRelBuilderFactory(env), timestampScore);
    if (fixTimestamp) {
      Check.state(timestampScore.isPresent(), null, null);
      Check.state(vtable.isRoot() && vtable.getAddedColumns().isEmpty(), null, null);
      QueryRelationalTable baseTbl = ((VirtualRelationalTable.Root)vtable).getBase();
      baseTbl.getTimestamp().fixTimestamp(baseTbl.getNumColumns()-1); //Timestamp must be last column
    }

    SQRLTable table = getContext(env, op.statement)
        .orElseThrow(()->new RuntimeException("Cannot resolve table"));
    Column column = env.variableFactory.addColumn(op.getStatement().getNamePath().getLast(),
        table, c.getDataType());
    //todo shadowing
    env.fieldMap.put(column, op.getStatement().getNamePath().getLast().getCanonical());
  }

  private void updateJoinMapping(Env env, StatementOp op) {
    Optional<VirtualRelationalTable> t = getTargetRelTable(env, op);
    Check.state(t.isPresent(), null, null);

    //op is a join, we need to discover the /to/ relationship
    Optional<SQRLTable> table = getContext(env, op.statement);
    JoinDeclarationFactory joinDeclarationFactory = new JoinDeclarationFactory(env);
    SqlJoinDeclaration joinDeclaration = joinDeclarationFactory.create(t.get(),
        op.relNode, op.validatedSql);
    VirtualRelationalTable vt =
        joinDeclarationFactory.getToTable(op.sqlValidator,
            op.validatedSql);
    Multiplicity multiplicity =
        joinDeclarationFactory.deriveMultiplicity(op.relNode);
    SQRLTable toTable = getSQRLTableFromVt(env, vt);
    Relationship relationship =
        env.variableFactory.addJoinDeclaration(op.statement.getNamePath(), table.get(),
            toTable, multiplicity);

    env.fieldMap.put(relationship, relationship.getName().getCanonical());
    env.resolvedJoinDeclarations.put(relationship, joinDeclaration);
  }

  private SQRLTable getSQRLTableFromVt(Env env, VirtualRelationalTable vt) {
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : env.getTableMap().entrySet()) {
      if (entry.getValue() == vt) {
        return entry.getKey();
      }
    }
    throw new RuntimeException();
  }

  private void createTable(Env env, StatementOp op) {
    List<Name> fieldNames = op.relNode.getRowType().getFieldList().stream()
        .map(f -> Name.system(f.getName())).collect(Collectors.toList());

    SQRLLogicalPlanConverter.RelMeta processedRel = optimize(env, op);

    ScriptTableDefinition queryTable = tableFactory.defineTable(op.statement.getNamePath(), processedRel, fieldNames);
    registerScriptTable(env, queryTable);
  }

  private AddedColumn createColumnAddOp(Env env, StatementOp op) {
    String columnName = op.statement.getNamePath().getLast().getCanonical();

    //TODO: check for sub-queries
    if (isSimple(op)) {
      Project project = (Project) op.getRelNode();
      return new Simple(columnName, project.getProjects().get(project.getProjects().size() - 1));
    }

    return new Complex(columnName, op.relNode);
  }

  private boolean isSimple(StatementOp op) {
    RelNode relNode = op.getRelNode();
    return relNode instanceof Project && relNode.getInput(0) instanceof TableScan;
  }

  private Optional<VirtualRelationalTable> getTargetRelTable(Env env, StatementOp op) {
    Optional<SQRLTable> context = getContext(env, op.statement);
    return context.map(c -> env.getTableMap().get(c));
  }

  public enum StatementKind {
    EXPR, QUERY, JOIN, SUBSCRIPTION, IMPORT, EXPORT, DISTINCT_ON
  }

  enum OpKind {
    EXPR, QUERY, ROOT_QUERY, EXPR_QUERY, JOIN, SUBSCRIPTION, IMPORT_TIMESTAMP
  }

  @Getter
  @Setter
  public class StatementOp {

    RelNode relNode;
    StatementKind statementKind;

    OpKind kind;
    SqrlStatement statement;
    boolean expression;
    Map fieldMapping;

    SqrlValidatorImpl sqrlValidator;
    SqlValidator sqlValidator;

    SqlNode query;
    SqlNode validatedSql;

    StatementOp(SqrlStatement statement, SqlNode query, StatementKind statementKind) {
      this.statement = statement;
      this.query = query;
      this.statementKind = statementKind;
    }
  }

  //
  // /**
  // * In order to expose a hierarchical table in Calcite, we need to register the
  // source dataset with
  // * the full calcite schema and a table for each nested record, which we can
  // then query.
  // * <p>
  // * To do this, we use a process called table shredding. This involves removing
  // the nested records
  // * from the source dataset, and then registering the resulting table with the
  // calcite schema.
  // * <p>
  // * We can then expand this into a full logical plan using the
  // * {@link ai.datasqrl.plan.calcite.rules.SqrlExpansionRelRule}
  // */
  // @Override
//   public Void visitImportDefinition(ImportDefinition node, Scope context) {
//   if (node.getNamePath().size() != 2) {
//   throw new RuntimeException(
//   String.format("Invalid import identifier: %s", node.getNamePath()));
//   }
//
//   //Check if this imports all or a single table
//   Name sourceDataset = node.getNamePath().get(0);
//   Name sourceTable = node.getNamePath().get(1);
//
//   List<TableImport> importTables;
//   Optional<Name> nameAlias = node.getAliasName();
//   List<ScriptTableDefinition> dt = new ArrayList<>();
//   if (sourceTable.equals(ReservedName.ALL)) { //import all tables from dataset
//   importTables = importManager.importAllTables(sourceDataset, schemaSettings,
//   errors);
//   nameAlias = Optional.empty();
//   throw new RuntimeException("TBD");
//   } else { //importing a single table
//   //todo: Check if table exists
//   TableImport tblImport = importManager.importTable(sourceDataset, sourceTable,
//   schemaSettings,
//   errors);
//   if (!tblImport.isSource()) {
//   throw new RuntimeException("TBD");
//   }
//   SourceTableImport sourceTableImport = (SourceTableImport) tblImport;
//
//   ScriptTableDefinition importedTable =
//   tableFactory.importTable(sourceTableImport, nameAlias,
//   planner.getRelBuilder());
//   dt.add(importedTable);
//   }
//
//   for (ScriptTableDefinition d : dt) {
//   registerScriptTable(d);
//   }
//
//   Check.state(!(node.getTimestamp().isPresent() &&
//   sourceTable.equals(ReservedName.ALL)), node,
//   Errors.TIMESTAMP_NOT_ALLOWED);
//   if (node.getTimestamp().isPresent()) {
//     String query = String.format("SELECT %s FROM %s",
//     NodeFormatter.accept(node.getTimestamp().get()),
//     dt.get(0).getTable().getName().getDisplay());
//     TranspiledResult result = transpile(query, Optional.empty(),
//     TranspileOptions.builder().build());
//     System.out.println(result.getRelNode().explain());
//   }
//
//   return null;
//   }
//
//   private void registerScriptTable(ScriptTableDefinition tblDef) {
//   //Update table mapping from SQRL table to Calcite table...
//   tblDef.getShredTableMap().values().stream().forEach(vt ->
//   relSchema.add(vt.getNameId(), vt));
//   relSchema.add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
//   this.tableMapper.getTableMap().putAll(tblDef.getShredTableMap());
//   //and also map all fields
//   this.fieldNames.putAll(tblDef.getFieldNameMap());
//
//   //Add all join declarations
//   tblDef.getShredTableMap().keySet().stream().flatMap(t ->
//   t.getAllRelationships()).forEach(r -> {
//   SqlJoinDeclaration dec =
//   joinDeclarationFactory.createParentChildJoinDeclaration(r);
//   joinDecs.add(r, dec);
//   });
//   if (tblDef.getTable().getPath().size() == 1) {
//   sqrlSchema.add(tblDef.getTable().getName().getDisplay(), (Table)
//   tblDef.getTable());
//   }
//   }
  //
  // @SneakyThrows
  // @Override
  // public Void visitDistinctAssignment(DistinctAssignment node, Scope context) {
  // TranspiledResult result = transpile(node.getSqlQuery(), Optional.empty(),
  // TranspileOptions.builder().build());
  // System.out.println(result.relNode.explain());
  //// mapping.addQuery(relNode, context.getTable(), node.getNamePath());
  // return null;
  // }
  //
  //
  // @SneakyThrows
  // @Override
  // public Void visitJoinAssignment(JoinAssignment node, Scope context) {
  //// Check.state(canAssign(node.getNamePath()), node,
  // Errors.UNASSIGNABLE_TABLE);
  // Check.state(node.getNamePath().size() > 1, node, Errors.JOIN_ON_ROOT);
  // Optional<SQRLTable> table = getContext(node.getNamePath().popLast());
  // Preconditions.checkState(table.isPresent());
  // String query = "SELECT * FROM _ " + node.getQuery();
  // TableWithPK pkTable = tableMapper.getTable(table.get());
  //
  // TranspiledResult result = transpile(query, table,
  // TranspileOptions.builder().orderToOrdinals(false).build());
  //
  // SqlJoinDeclaration joinDeclaration = joinDeclarationFactory.create(pkTable,
  // result);
  // VirtualRelationalTable vt =
  // joinDeclarationFactory.getToTable(result.getSqlValidator(),
  // result.getSqlNode());
  // Multiplicity multiplicity =
  // joinDeclarationFactory.deriveMultiplicity(result.getRelNode());
  // SQRLTable toTable = tableMapper.getScriptTable(vt);
  // Relationship relationship =
  // variableFactory.addJoinDeclaration(node.getNamePath(), table.get(),
  // toTable, multiplicity);
  //
  // //todo: assert non-shadowed
  // this.fieldNames.put(relationship, relationship.getName().getCanonical());
  // this.joinDecs.add(relationship, joinDeclaration);
  // return null;
  // }
  //
  // private Optional<SQRLTable> getContext(NamePath namePath) {
  // if (namePath.size() == 0) {
  // return Optional.empty();
  // }
  // SQRLTable table = (SQRLTable)
  // sqrlSchema.getTable(namePath.get(0).getDisplay(), false)
  // .getTable();
  //
  // return table.walkTable(namePath.popFirst());
  // }
  //
  // @SneakyThrows
  // private TranspiledResult transpile(String query, Optional<SQRLTable> context,
  // TranspileOptions options) {
  // SqlNode node = SqlParser.create(query,
  // SqlParser.config().withCaseSensitive(false).withUnquotedCasing(Casing.UNCHANGED))
  // .parseQuery();
  //
  // SqrlValidateResult result = validate(node, context);
  // TranspiledResult transpiledResult = transpile(result.getValidated(),
  // result.getValidator(),
  // options);
  // return transpiledResult;
  // }
  //
  // private TranspiledResult transpile(SqlNode node, SqrlValidatorImpl validator,
  // TranspileOptions options) {
  // SqlSelect select =
  // node instanceof SqlSelect ? (SqlSelect) node : (SqlSelect) ((SqlOrderBy)
  // node).query;
  // SqlValidatorScope scope = validator.getSelectScope(select);
  //
  // Transpile transpile = new Transpile(validator, tableMapper,
  // uniqueAliasGenerator, joinDecs,
  // sqlNodeBuilder, () -> new JoinBuilderImpl(uniqueAliasGenerator, joinDecs,
  // tableMapper),
  // fieldNames, options);
  //
  // transpile.rewriteQuery(select, scope);
  //
  // SqlValidator sqlValidator = TranspilerFactory.createSqlValidator(relSchema);
  // SqlNode validated = sqlValidator.validate(select);
  //
  // return new TranspiledResult(select, validator, node, sqlValidator,
  // plan(validated, sqlValidator));
  // }
  //
  // private SqrlValidateResult validate(SqlNode node, Optional<SQRLTable>
  // context) {
  // SqrlValidatorImpl sqrlValidator =
  // TranspilerFactory.createSqrlValidator(sqrlSchema);
  // sqrlValidator.setContext(context);
  // sqrlValidator.validate(node);
  // return new SqrlValidateResult(node, sqrlValidator);
  // }
  //
  // @Override
  // public Void visitExpressionAssignment(ExpressionAssignment node, Scope
  // context) {
  // Optional<SQRLTable> table = getContext(node.getNamePath().popLast());
  // Preconditions.checkState(table.isPresent());
  // TranspiledResult result = transpile("SELECT " + node.getSql() + " FROM _",
  // table,
  // TranspileOptions.builder().build());
  //
  // return null;
  // }
  //
  // @Override
  // public Void visitQueryAssignment(QueryAssignment node, Scope context) {
  // Optional<SQRLTable> ctx = getContext(node.getNamePath().popLast());
  // TranspiledResult result = transpile(node.getSql(), ctx,
  // TranspileOptions.builder().build());
  // RelNode relNode = result.getRelNode();
  // System.out.println(result.relNode.explain());
  //// Check.state(canAssign(node.getNamePath()), node,
  // Errors.UNASSIGNABLE_QUERY_TABLE);
  //
  // NamePath namePath = node.getNamePath();
  //
  // boolean isExpression = false;
  //
  // if (isExpression) {
  // Check.state(node.getNamePath().size() > 1, node,
  // Errors.QUERY_EXPRESSION_ON_ROOT);
  // SQRLTable table = ctx.get();
  // Column column = variableFactory.addQueryExpression(namePath, table);
  // VirtualRelationalTable t = (VirtualRelationalTable) relSchema.getTable(
  // this.tableMapper.getTable(table).getNameId(), false).getTable();
  // RelDataTypeField field = relNode.getRowType().getFieldList()
  // .get(relNode.getRowType().getFieldCount() - 1);
  // RelDataTypeField newField = new RelDataTypeFieldImpl(
  // node.getNamePath().getLast().getCanonical(),
  // t.getRowType(null).getFieldCount(),
  // field.getValue());
  //
  // this.fieldNames.put(column, newField.getName());
  //
  // AddedColumn addedColumn = new Complex(newField.getName(),
  // result.getRelNode());
  // t.addColumn(addedColumn, new SqrlTypeFactory(new SqrlTypeSystem()));
  //
  // } else {
  // List<Name> fieldNames = relNode.getRowType().getFieldList().stream()
  // .map(f -> Name.system(f.getName())).collect(Collectors.toList());
  // SQRLLogicalPlanConverter.RelMeta processedRel = optimize(relNode);
  // ScriptTableDefinition queryTable = tableFactory.defineTable(namePath,
  // processedRel,
  // fieldNames);
  // registerScriptTable(queryTable);
  // Optional<Pair<Relationship, Relationship>> childRel =
  // variableFactory.linkParentChild(
  // namePath,
  // queryTable.getTable(), ctx);
  //
  // childRel.ifPresent(rel -> {
  // joinDecs.add(rel.getLeft(),
  // joinDeclarationFactory.createChild(rel.getLeft()));
  // joinDecs.add(rel.getRight(),
  // joinDeclarationFactory.createParent(rel.getRight()));
  // });
  // }
  //
  // return null;
  // }
  //
  // public SQRLLogicalPlanConverter.RelMeta optimize(RelNode relNode) {
  // System.out.println("LP$0: \n" + relNode.explain());
  //
  // //Step 1: Push filters into joins so we can correctly identify self-joins
  // relNode = planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN,
  // relNode);
  // System.out.println("LP$1: \n" + relNode.explain());
  //
  // //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
  // //self-joins (including nested self-joins) as well as infer primary keys,
  // //table types, and timestamps in the process
  //
  // SQRLLogicalPlanConverter sqrl2sql = new SQRLLogicalPlanConverter(
  // () -> planner.getRelBuilder(), new
  // SqrlRexUtil(planner.getRelBuilder().getRexBuilder()));
  // relNode = relNode.accept(sqrl2sql);
  // System.out.println("LP$2: \n" + relNode.explain());
  // return sqrl2sql.putPrimaryKeysUpfront(sqrl2sql.getRelHolder(relNode));
  // }
  //
  // @SneakyThrows
  // private RelNode plan(SqlNode sqlNode, SqlValidator sqlValidator) {
  // System.out.println(sqlNode);
  // planner.refresh();
  // planner.setValidator(sqlNode, sqlValidator);
  //
  // RelRoot root = planner.rel(sqlNode);
  // RelNode relNode = root.rel;
  //
  // return relNode;
  // }
  //
  // @Override
  // public Table getTable(String tableName) {
  // return relSchema.getTable(tableName, false).getTable();
  // }
  //
  // public OptimizedDAG planDAG() {
  // DAGPlanner dagPlanner = new DAGPlanner();
  // //For now, we are going to generate simple scan queries for each virtual
  // table
  // //TODO: this needs to be replaced by the queries generated from the GraphQL
  // API
  // List<APIQuery> apiQueries =
  // CalciteUtil.getTables(relSchema,VirtualRelationalTable.class).stream()
  // .map(vt -> new
  // APIQuery("query_"+vt.getNameId(),planner.getRelBuilder().scan(vt.getNameId()).build()))
  // .collect(Collectors.toList());
  // return dagPlanner.plan(relSchema, planner, apiQueries);
  // }
  //
  // @Value
  // class SqrlValidateResult {
  //
  // SqlNode validated;
  // SqrlValidatorImpl validator;
  // }
  //
  // @Value
  // public class TranspiledResult {
  //
  // SqlNode sqrlNode;
  // SqrlValidatorImpl sqrlValidator;
  // SqlNode sqlNode;
  // SqlValidator sqlValidator;
  // RelNode relNode;
  // }
  //
  // @Value
  // class Scope {
  //
  // }
}
