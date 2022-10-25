package ai.datasqrl.plan.local.generate;

import ai.datasqrl.compile.loaders.DataSourceLoader;
import ai.datasqrl.compile.loaders.JavaFunctionLoader;
import ai.datasqrl.compile.loaders.Loader;
import ai.datasqrl.compile.loaders.TypeLoader;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.TranspilerFactory;
import ai.datasqrl.plan.calcite.hints.ExecutionHint;
import ai.datasqrl.plan.calcite.rules.AnnotatedLP;
import ai.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.calcite.table.AddedColumn.Simple;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.transpile.*;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
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

  @Getter
  public class Env {

    CalciteTableFactory tableFactory = new CalciteTableFactory(PlannerFactory.getTypeFactory());
    SchemaAdjustmentSettings schemaAdjustmentSettings = SchemaAdjustmentSettings.DEFAULT;
    List<Loader> loaders = List.of(new DataSourceLoader(), new JavaFunctionLoader(),
        new TypeLoader());
    VariableFactory variableFactory = new VariableFactory();
    List<StatementOp> ops = new ArrayList<>();
    List<SqrlStatement> queryOperations = new ArrayList<>();

    // Mappings
    Map<SQRLTable, VirtualRelationalTable> tableMap = new HashMap<>();
    Map<Field, String> fieldMap = new HashMap<>();
    SqrlCalciteSchema userSchema;
    SqrlCalciteSchema relSchema;

    Session session;
    ScriptNode scriptNode;
    URI packageUri;
    ExecutionPipeline pipeline;

    //Updated while processing each node
    SqlNode currentNode = null;

    public Env(SqrlCalciteSchema relSchema, Session session,
        ScriptNode scriptNode, URI packageUri) {
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
        session.planner.getDefaultSchema().unwrap(SqrlCalciteSchema.class),
        session,
        script,
        basePath.toUri()
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
    throw new RuntimeException("Cannot find loader for module: " + uri + "." + name);
  }

  public static void registerScriptTable(Env env, ScriptTableDefinition tblDef) {
    //Update table mapping from SQRL table to Calcite table...

    tblDef.getShredTableMap().values().stream().forEach(vt ->
        env.relSchema.add(vt.getNameId(), vt));
    env.relSchema.add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
    if (tblDef.getBaseTable() instanceof ProxyImportRelationalTable) {
      ImportedSourceTable impTable = ((ProxyImportRelationalTable) tblDef.getBaseTable()).getSourceTable();
      env.relSchema.add(impTable.getNameId(), impTable);
    }
    env.tableMap.putAll(tblDef.getShredTableMap());
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap()
        .entrySet()) {
      entry.getKey().setVT(entry.getValue());
      entry.getValue().setSqrlTable(entry.getKey());
    }

    //and also map all fields
    env.fieldMap.putAll(tblDef.getFieldNameMap());

    if (tblDef.getTable().getPath().size() == 1) {
//      env.userSchema.add(tblDef.getTable().getName().getDisplay(), (org.apache.calcite.schema.Table)
//          tblDef.getTable());
      env.relSchema.add(tblDef.getTable().getName().getDisplay(), (org.apache.calcite.schema.Table)
          tblDef.getTable());
    } else {

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
      sqlNode = transformExpressionToQuery(env, statement, sqlNode);
      statementKind = StatementKind.EXPR;
    } else if (statement instanceof StreamAssignment) {
      sqlNode = ((StreamAssignment) statement).getQuery();
      statementKind = StatementKind.STREAM;
    } else if (statement instanceof QueryAssignment) {
      sqlNode = ((QueryAssignment) statement).getQuery();
      statementKind = StatementKind.QUERY;
    } else if (statement instanceof CreateSubscription) {
      sqlNode = ((CreateSubscription) statement).getQuery();
      statementKind = StatementKind.SUBSCRIPTION;
    } else if (statement instanceof DistinctAssignment) {
      sqlNode = ((DistinctAssignment) statement).getQuery();
      statementKind = StatementKind.DISTINCT_ON;
    } else if (statement instanceof JoinAssignment) {
      sqlNode = ((JoinAssignment) statement).getQuery();
      statementKind = StatementKind.JOIN;
    } else if (statement instanceof ImportDefinition) {
      ImportDefinition importDef = (ImportDefinition) statement;
      if (importDef.getTimestamp().isEmpty()) {
        return Optional.empty();
      }
      sqlNode = importDef.getTimestamp().get();
      sqlNode = transformExpressionToQuery(env, statement, sqlNode);
      statementKind = StatementKind.IMPORT;

    } else {
      throw new RuntimeException("Unrecognized assignment type");
    }

    StatementOp op = new StatementOp(statement, sqlNode, statementKind);
    env.ops.add(op);
    return Optional.of(op);
  }

  private SqlNode transformExpressionToQuery(Env env, SqrlStatement statement, SqlNode sqlNode) {
    Preconditions.checkState(getContext(env, statement).isPresent());
    return new SqlSelect(SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        new SqlNodeList(List.of(sqlNode), SqlParserPos.ZERO),
        null,
        null,
        null,
        null,
        SqlNodeList.EMPTY,
        null,
        null,
        null,
        SqlNodeList.EMPTY
    );
  }

  public void validate(Env env, StatementOp op) {
    // Validator validator = createValidator(op);
    // validator.validate(env, op);
  }

  public void transpile(Env env, StatementOp op) {
    List<String> assignmentPath = op.getStatement().getNamePath().popLast()
        .stream()
        .map(e -> e.getCanonical())
        .collect(Collectors.toList());

    Optional<SQRLTable> table = getContext(env, op.getStatement());
    Optional<VirtualRelationalTable> context =
        table.map(t -> t.getVt());

    Function<SqlNode, Analysis> analyzer = (node) -> new AnalyzeStatement(env.relSchema, assignmentPath)
        .accept(node);

    SqlNode node = context.map(
        c -> new AddContextTable(c.getNameId()).accept(op.getQuery())).orElse(op.getQuery());

    Analysis currentAnalysis = analyzer.apply(node);

    List<Function<Analysis, SqlShuttle>> transforms = List.of(
        QualifyIdentifiers::new,
        FlattenFieldPaths::new,
        FlattenTablePaths::new,
        ReplaceWithVirtualTable::new
    );

    for (Function<Analysis, SqlShuttle> transform : transforms) {
      node = node.accept(transform.apply(currentAnalysis));
      currentAnalysis = analyzer.apply(node);
    }

    final SqlNode finalStage = node;

    SqlValidator sqrlValidator = TranspilerFactory.createSqrlValidator(env.relSchema,
        assignmentPath, true);
    sqrlValidator.validate(node);

    if (op.getStatementKind() == StatementKind.DISTINCT_ON) {

      new AddHints(sqrlValidator, context).accept(op, finalStage);

      SqlValidator validate2 = TranspilerFactory.createSqrlValidator(env.relSchema,
          assignmentPath, false);
      validate2.validate(finalStage);
      op.setQuery(finalStage);
      op.setSqrlValidator(validate2);
    } else {
      SqlNode rewritten = new AddContextFields(sqrlValidator, context).accept(finalStage);

      //Skip this for joins, we'll add the hints later when we reconstruct the node from the relnode
      // Hints don't carry over when moving from rel -> sqlnode
      if (op.getStatementKind() != StatementKind.JOIN) {
        SqlValidator prevalidate = TranspilerFactory.createSqrlValidator(env.relSchema,
            assignmentPath, true);
        prevalidate.validate(rewritten);
        new AddHints(prevalidate, context).accept(op, rewritten);
      }

      SqlValidator validator = TranspilerFactory.createSqrlValidator(env.relSchema,
          assignmentPath, false);
      SqlNode newNode2 = validator.validate(rewritten);
      op.setQuery(newNode2);
      op.setSqrlValidator(validator);
    }
  }

  private Optional<SQRLTable> getContext(Env env, SqrlStatement statement) {
    if (statement.getNamePath().size() <= 1) {
      return Optional.empty();
    }
    Optional<SQRLTable> table =
        Optional.ofNullable(env.relSchema.getTable(statement.getNamePath().get(0).getDisplay(), false))
            .map(t->(SQRLTable)t.getTable());

    if (statement.getNamePath().popFirst().size() == 1) {
      return table;
    }
    if (table.isEmpty()) {
      //No table in namespace
      throw new RuntimeException("No table in namespace: " + statement.getNamePath().popLast());
    }
    return table.get().walkTable(statement.getNamePath().popFirst().popLast());
  }

  public void plan(Env env, StatementOp op) {
    env.session.planner.refresh();
    env.session.planner.setValidator(op.getQuery(), op.getSqrlValidator());

    RelNode relNode = env.session.planner.rel(op.getQuery()).rel;

    //Optimization prepass
    relNode = env.session.planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN,
        relNode);

    op.setRelNode(relNode);
  }

  public AnnotatedLP optimize(Env env, StatementOp op) {
    List<String> fieldNames = op.relNode.getRowType().getFieldNames();
//    System.out.println("LP$0: \n" + op.relNode.explain());

    //Step 1: Push filters into joins so we can correctly identify self-joins
    RelNode relNode = env.session.planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN,
        op.relNode);
//    System.out.println("LP$1: \n" + relNode.explain());

    //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
    //self-joins (including nested self-joins) as well as infer primary keys,
    //table types, and timestamps in the process


    Supplier<RelBuilder> relBuilderFactory = getRelBuilderFactory(env);
    AnnotatedLP prel;
    SQRLLogicalPlanConverter.Config.ConfigBuilder converterConfig = SQRLLogicalPlanConverter.Config.builder();
    Optional<ExecutionHint> execHint = ExecutionHint.fromSqlHint(op.getStatement().getHints());
    if (execHint.isPresent()) {
      converterConfig = execHint.get().getConfig(env.session.getPipeline(), converterConfig);
      prel = SQRLLogicalPlanConverter.convert(relNode, relBuilderFactory, converterConfig.build());
    } else {
      prel = SQRLLogicalPlanConverter.findCheapest(relNode, env.session.pipeline, relBuilderFactory);
    }
    if (op.statementKind == StatementKind.DISTINCT_ON) {
      //Get all field names from relnode
      fieldNames = prel.relNode.getRowType().getFieldNames();
    }
    prel = prel.postProcess(relBuilderFactory.get(),fieldNames);
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
        addColumn(env, op, c, op.kind == IMPORT_TIMESTAMP);
        break;
      case ROOT_QUERY:
        createTable(env, op, Optional.empty());
        break;
      case QUERY:
        Optional<SQRLTable> parentTable = getContext(env, op.getStatement());
        assert parentTable.isPresent();
        createTable(env, op, parentTable);

        break;
      case JOIN:
        updateJoinMapping(env, op);
    }
  }

  private void addColumn(Env env, StatementOp op, AddedColumn c, boolean fixTimestamp) {
    Optional<VirtualRelationalTable> optTable = getTargetRelTable(env, op);
//    Check.state(optTable.isPresent(), null, null);
    SQRLTable table = getContext(env, op.statement)
        .orElseThrow(() -> new RuntimeException("Cannot resolve table"));
    Name name = op.getStatement().getNamePath().getLast();
    if (table.getField(name).isPresent()) {
      name = Name.system(op.getStatement().getNamePath().getLast().getCanonical() + "_");
    }
    c.setNameId(name.getCanonical());

    VirtualRelationalTable vtable = optTable.get();
    Optional<Integer> timestampScore = env.tableFactory.getTimestampScore(
        op.statement.getNamePath().getLast(), c.getDataType());
    vtable.addColumn(c, env.tableFactory.getTypeFactory(), getRelBuilderFactory(env),
        timestampScore);
    if (fixTimestamp) {
//      Check.state(timestampScore.isPresent(), null, null);
//      Check.state(vtable.isRoot() && vtable.getAddedColumns().isEmpty(), null, null);
      QueryRelationalTable baseTbl = ((VirtualRelationalTable.Root) vtable).getBase();
      baseTbl.getTimestamp()
          .getCandidateByIndex(baseTbl.getNumColumns() - 1) //Timestamp must be last column
          .fixAsTimestamp();
    }

    Column column = env.variableFactory.addColumn(name,
        table, c.getDataType());
    //todo shadowing
    env.fieldMap.put(column, name.getCanonical());
  }

  private void updateJoinMapping(Env env, StatementOp op) {
    //op is a join, we need to discover the /to/ relationship
    Optional<SQRLTable> table = getContext(env, op.statement);
    JoinDeclarationUtil joinDeclarationUtil = new JoinDeclarationUtil(env);
    SQRLTable toTable =
        joinDeclarationUtil.getToTable(op.sqrlValidator,
            op.query);
    Multiplicity multiplicity =
        joinDeclarationUtil.deriveMultiplicity(op.relNode);

    SqlNode node = pullWhereIntoJoin(op.getQuery());

    env.variableFactory.addJoinDeclaration(op.statement.getNamePath(), table.get(),
        toTable, multiplicity, node);
  }

  private SqlNode pullWhereIntoJoin(SqlNode query) {
    if (query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) query;
      if (select.getWhere() != null) {
        SqlJoin join = (SqlJoin) select.getFrom();
        FlattenTablePaths.addJoinCondition(join, select.getWhere());
      }
      return select.getFrom();
    }
    return query;
  }

  private void createTable(Env env, StatementOp op, Optional<SQRLTable> parentTable) {

    final AnnotatedLP processedRel = optimize(env, op);

    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> Name.system(n)).collect(Collectors.toList());

    Optional<Pair<SQRLTable, VirtualRelationalTable>> parentPair = parentTable.map(tbl ->
        Pair.of(tbl, env.tableMap.get(tbl)));
    ScriptTableDefinition queryTable = env.tableFactory.defineTable(op.statement.getNamePath(),
        processedRel,
        fieldNames, parentPair);
    registerScriptTable(env, queryTable);
  }


  private AddedColumn createColumnAddOp(Env env, StatementOp op) {
    String columnName = op.statement.getNamePath().getLast().getCanonical();

    //TODO: check for sub-queries
    if (isSimple(op)) {
      Project project = (Project) op.getRelNode();
      return new Simple(columnName, project.getProjects().get(project.getProjects().size() - 1));
    } else {
      //return new Complex(columnName, op.relNode);
      throw new UnsupportedOperationException("Complex column not yet supported");
    }


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
    EXPR, QUERY, JOIN, SUBSCRIPTION, IMPORT, EXPORT, DISTINCT_ON, STREAM
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

    SqlValidator sqrlValidator;
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
