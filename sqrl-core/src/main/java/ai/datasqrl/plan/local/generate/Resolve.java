package ai.datasqrl.plan.local.generate;

import ai.datasqrl.compile.loaders.DataSourceLoader;
import ai.datasqrl.compile.loaders.JavaFunctionLoader;
import ai.datasqrl.compile.loaders.Loader;
import ai.datasqrl.compile.loaders.TypeLoader;
import ai.datasqrl.errors.ErrorCode;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
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
import java.io.File;
import java.net.URI;
import java.nio.file.Path;
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

import static ai.datasqrl.errors.ErrorCode.IMPORT_CANNOT_BE_ALIASED;
import static ai.datasqrl.errors.ErrorCode.IMPORT_STAR_CANNOT_HAVE_TIMESTAMP;
import static ai.datasqrl.plan.local.generate.Resolve.OpKind.IMPORT_TIMESTAMP;

@Getter
public class Resolve {

  private final Path basePath;

  public Resolve(Path basePath) {
    this.basePath = basePath;
  }

  enum ImportState {
    UNRESOLVED, RESOLVING, RESOLVED;
  }

  @Getter
  public class Env {
    CalciteTableFactory tableFactory = new CalciteTableFactory(
        new SqrlTypeFactory(new SqrlTypeSystem()));
    SchemaAdjustmentSettings schemaAdjustmentSettings = SchemaAdjustmentSettings.DEFAULT;
    List<Loader> loaders = List.of(new DataSourceLoader(), new JavaFunctionLoader(), new TypeLoader());
    VariableFactory variableFactory = new VariableFactory();
    List<StatementOp> ops = new ArrayList<>();
    Map<Relationship, SqlJoinDeclaration> resolvedJoinDeclarations = new HashMap<>();
    List<SqrlStatement> queryOperations = new ArrayList<>();

    // Mappings
    Map<SQRLTable, VirtualRelationalTable> tableMap = new HashMap<>();
    Map<Field, String> fieldMap = new HashMap<>();
    UniqueAliasGenerator aliasGenerator = new UniqueAliasGeneratorImpl();

    SqrlCalciteSchema userSchema;
    CalciteSchema relSchema;

    Session session;
    ScriptNode scriptNode;
    URI packageUri;

    //Updated while processing each node
    SqlNode currentNode = null;

    public Env(SqrlCalciteSchema userSchema, CalciteSchema relSchema, Session session,
        ScriptNode scriptNode, URI packageUri) {
      this.userSchema = userSchema;
      this.relSchema = relSchema;
      this.session = session;
      this.scriptNode = scriptNode;
      this.packageUri = packageUri;
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
        createUserSchema(),
        session.planner.getDefaultSchema().unwrap(CalciteSchema.class),
        session,
        script,
        basePath.toUri().resolve("build/")
    );
  }

  private SqrlCalciteSchema createUserSchema() {
    return new SqrlCalciteSchema(CalciteSchema.createRootSchema(true).schema);
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

  //TODO: Allow modules to resolve other modules, for example a library package
  private void resolveImportDefinition(Env env, ImportDefinition node) {
    setCurrentNode(env, node);
    NamePath path = node.getImportPath();

    //Walk path, check if last item is ALL.
    URI uri = env.getPackageUri();
    for (int i = 0; i < path.getNames().length - 1; i++) {
      Name name = path.getNames()[i];
      uri = uri.resolve(name.getCanonical() + "/");
    }

    Name last = path.getNames()[path.getNames().length - 1];
    /*
     * Add all files to namespace
     */
    if (last.equals(ReservedName.ALL)) {
      Check.state(node.getAlias().isEmpty(), IMPORT_CANNOT_BE_ALIASED,
          env.getCurrentNode(), () -> "Alias `" + node.getAlias().get().getCanonical() +
              "` cannot be used here.");

      Check.state(node.getTimestamp().isEmpty(),
          IMPORT_STAR_CANNOT_HAVE_TIMESTAMP,
          env.getCurrentNode(),
          () -> node.getTimestamp().get().getParserPosition(),
          () -> "Cannot use timestamp with import star");

      File file = new File(uri);
      Preconditions.checkState(file.isDirectory(), "Import * is not a directory");

      for (String f : file.list()) {
        loadModuleFile(env, uri, f);
      }
    } else {
      uri = uri.resolve(last.getCanonical());
      File file = new File(uri);
      Preconditions.checkState(!file.isDirectory());
      //load a Named module
      loadSingleModule(env,
          uri,
          path.getLast().getCanonical(),
          node.getAlias());
    }
  }

  private void setCurrentNode(Env env, SqlNode node) {
    env.currentNode = node;
  }

  private void loadModuleFile(Env env, URI uri, String name) {
    for (Loader loader : env.getLoaders()) {
      if (loader.handlesFile(uri, name)) {
        loader.loadFile(env, uri, name);
        return;
      }
    }
  }

  private void loadSingleModule(Env env, URI uri, String name, Optional<Name> alias) {
    for (Loader loader : env.getLoaders()) {
      if (loader.handles(uri, name)) {
        loader.load(env, uri, name, alias);
        return;
      }
    }
    throw new RuntimeException("Cannot find loader for module");
  }

  public static void registerScriptTable(Env env, ScriptTableDefinition tblDef) {
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

      env.userSchema.add(tblDef.getTable().getName().getDisplay(), (org.apache.calcite.schema.Table)
          tblDef.getTable());
    } else {
      System.out.println();


    }
  }

  void planQueries(Env env) {
    // Go through each operation and resolve
    for (SqrlStatement q : env.queryOperations) {
      setCurrentNode(env, q);
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
    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(env.userSchema);
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
        env.userSchema.getTable(statement.getNamePath().get(0).getDisplay(), false)
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
    if (op.statementKind==StatementKind.DISTINCT_ON) {
      //Get all field names from relnode
      fieldNames = relNode.getRowType().getFieldNames();
    }
    SQRLLogicalPlanConverter.RelMeta prel = sqrl2sql.postProcess(sqrl2sql.getRelHolder(relNode), fieldNames);
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
//    Check.state(optTable.isPresent(), null, null);

    VirtualRelationalTable vtable = optTable.get();
    Optional<Integer> timestampScore = env.tableFactory.getTimestampScore(op.statement.getNamePath().getLast(),c.getDataType());
    vtable.addColumn(c, env.tableFactory.getTypeFactory(), getRelBuilderFactory(env), timestampScore);
    if (fixTimestamp) {
//      Check.state(timestampScore.isPresent(), null, null);
//      Check.state(vtable.isRoot() && vtable.getAddedColumns().isEmpty(), null, null);
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
//    Check.state(t.isPresent(), null, null);

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

    final SQRLLogicalPlanConverter.RelMeta processedRel = optimize(env, op);

    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream().map(idx -> relFieldNames.get(idx))
            .map(n -> Name.system(n)).collect(Collectors.toList());

    ScriptTableDefinition queryTable = env.tableFactory.defineTable(op.statement.getNamePath(), processedRel, fieldNames);
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
}
