package ai.datasqrl.plan.local.generate;

import ai.datasqrl.compile.loaders.*;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.physical.ExecutionEngine;
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
import ai.datasqrl.schema.Relationship;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.datasqrl.config.error.ErrorCode.IMPORT_CANNOT_BE_ALIASED;
import static ai.datasqrl.config.error.ErrorCode.IMPORT_STAR_CANNOT_HAVE_TIMESTAMP;
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
    Loader loader = new CompositeLoader(new DataSourceLoader(), new JavaFunctionLoader(), new TypeLoader());
    List<StatementOp> ops = new ArrayList<>();
    List<SqrlStatement> queryOperations = new ArrayList<>();

    // Mappings
    Map<SQRLTable, VirtualRelationalTable> tableMap = new HashMap<>();
    SqrlCalciteSchema userSchema;
    SqrlCalciteSchema relSchema;

    Session session;
    ScriptNode scriptNode;
    Path packagePath;
    ExecutionPipeline pipeline;

    //Updated while processing each node
    SqlNode currentNode = null;

    public Env(SqrlCalciteSchema relSchema, Session session,
        ScriptNode scriptNode, Path packagePath) {
      this.relSchema = relSchema;
      this.session = session;
      this.scriptNode = scriptNode;
      this.packagePath = packagePath;
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
        basePath
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
    NamePath fullPath = node.getImportPath();
    Name last = fullPath.getLast();
    NamePath basePath = fullPath.subList(0, fullPath.size()-1);
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

      Collection<Name> loaded = env.getLoader().loadAll(env, basePath);
      Preconditions.checkState(!loaded.isEmpty(), "Import [%s] is not a package or package is empty",basePath);
    } else {
      boolean loaded = env.getLoader().load(env,fullPath, node.getAlias());
      Preconditions.checkState(loaded, "Could not import: %s", fullPath);
    }
  }

  private void setCurrentNode(Env env, SqlNode node) {
    env.currentNode = node;
  }


  public static void registerScriptTable(Env env, ScriptTableDefinition tblDef) {
    //Update table mapping from SQRL table to Calcite table...

    tblDef.getShredTableMap().values().stream().forEach(vt ->
        env.relSchema.add(vt.getNameId(), vt));
    env.relSchema.add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
    if (tblDef.getBaseTable() instanceof ProxySourceRelationalTable) {
      AbstractRelationalTable impTable = ((ProxySourceRelationalTable) tblDef.getBaseTable()).getBaseTable();
      env.relSchema.add(impTable.getNameId(), impTable);
    }
    env.tableMap.putAll(tblDef.getShredTableMap());
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap()
        .entrySet()) {
      entry.getKey().setVT(entry.getValue());
      entry.getValue().setSqrlTable(entry.getKey());
    }

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

      return OpKind.QUERY;
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

      if (importDef.getTimestampAlias().isPresent()) {
        sqlNode = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
            importDef.getTimestamp().get(), importDef.getTimestampAlias().get());
      } else {
        sqlNode = importDef.getTimestamp().get();
      }
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
    Preconditions.checkState(getContext(env, statement).isPresent(),"Could not find parent table: %s",statement.getNamePath());
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

    Function<SqlNode, Analysis> analyzer = (node) -> new AnalyzeStatement(env.relSchema,
        assignmentPath, table)
        .accept(node);

    SqlNode node = op.getQuery();
    Analysis currentAnalysis = null;

    List<Function<Analysis, SqlShuttle>> transforms = List.of(
        (analysis) -> new AddContextTable(table.isPresent()),
        QualifyIdentifiers::new,
        FlattenFieldPaths::new,
        FlattenTablePaths::new,
        ReplaceWithVirtualTable::new,
        AllowMixedFieldUnions::new
    );

    for (Function<Analysis, SqlShuttle> transform : transforms) {
      node = node.accept(transform.apply(currentAnalysis));
      currentAnalysis = analyzer.apply(node);
    }

    final SqlNode sql = node.accept(new ConvertJoinDeclaration(context));
    SqlValidator sqrlValidator = TranspilerFactory.createSqlValidator(env.relSchema
    );
    System.out.println(sql);
    sqrlValidator.validate(sql);

    if (op.getStatementKind() == StatementKind.DISTINCT_ON) {

      new AddHints(sqrlValidator, context).accept(op, sql);

      SqlValidator validate2 = TranspilerFactory.createSqlValidator(env.relSchema
      );
      validate2.validate(sql);
      op.setQuery(sql);
      op.setSqrlValidator(validate2);
    } else if (op.getStatementKind() == StatementKind.JOIN) {
      op.setQuery(sql);
      op.setJoinDeclaration((SqrlJoinDeclarationSpec) node);
      op.setSqrlValidator(sqrlValidator);
     } else {
      SqlNode rewritten = new AddContextFields(sqrlValidator, context,
          isAggregate(sqrlValidator, sql)).accept(sql);

      SqlValidator prevalidate = TranspilerFactory.createSqlValidator(env.relSchema
      );
      prevalidate.validate(rewritten);
      new AddHints(prevalidate, context).accept(op, rewritten);

      SqlValidator validator = TranspilerFactory.createSqlValidator(env.relSchema
      );
      SqlNode newNode2 = validator.validate(rewritten);
      op.setQuery(newNode2);
      op.setSqrlValidator(validator);
    }
  }

  private boolean isAggregate(SqlValidator sqrlValidator, SqlNode node) {
    if (node instanceof SqlSelect) {
      return sqrlValidator.isAggregate((SqlSelect) node);
    }
    return sqrlValidator.isAggregate(node);
  }

  private Optional<SQRLTable> getContext(Env env, SqrlStatement statement) {
    if (statement.getNamePath().size() <= 1) {
      return Optional.empty();
    }
    Optional<SQRLTable> table =
        Optional.ofNullable(env.relSchema.getTable(statement.getNamePath().get(0).getDisplay(), false))
            .map(t->(SQRLTable)t.getTable());
    if (table.isEmpty()) {
      //No table in namespace
      throw new RuntimeException("No table in namespace: " + statement.getNamePath().popLast());
    }
    if (statement.getNamePath().popFirst().size() == 1) {
      return table;
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

  public AnnotatedLP convert(Env env, StatementOp op) {
    List<String> fieldNames = op.relNode.getRowType().getFieldNames();

    //Step 1: Push filters into joins so we can correctly identify self-joins
    RelNode relNode = env.session.planner.transform(OptimizationStage.PUSH_FILTER_INTO_JOIN,
        op.relNode);

    //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
    //self-joins (including nested self-joins) as well as infer primary keys,
    //table types, and timestamps in the process
    Supplier<RelBuilder> relBuilderFactory = getRelBuilderFactory(env);
    final SQRLLogicalPlanConverter.Config.ConfigBuilder configBuilder = SQRLLogicalPlanConverter.Config.builder();
    AnnotatedLP prel;
    Optional<ExecutionHint> execHint = ExecutionHint.fromSqlHint(op.getStatement().getHints());
    if (op.statementKind==StatementKind.STREAM) {
      Preconditions.checkArgument(!execHint.filter(h -> h.getExecType() != ExecutionEngine.Type.STREAM).isPresent(),
              "Invalid execution hint: %s",execHint);
      if (execHint.isEmpty()) execHint = Optional.of(new ExecutionHint(ExecutionEngine.Type.STREAM));
    }
    execHint.map(h -> h.getConfig(env.session.getPipeline(), configBuilder));
    SQRLLogicalPlanConverter.Config config = configBuilder.build();
    if (config.getStartStage()!=null) {
      prel = SQRLLogicalPlanConverter.convert(relNode, relBuilderFactory, config);
    } else {
      prel = SQRLLogicalPlanConverter.findCheapest(op.statement.getNamePath(), relNode, relBuilderFactory, env.session.pipeline, config);
    }
    if (op.statementKind == StatementKind.DISTINCT_ON) {
      //Get all field names from resulting relnode instead of the op
      fieldNames = prel.relNode.getRowType().getFieldNames();
    }
    if (op.statementKind==StatementKind.STREAM) {
      prel = prel.postProcessStream(relBuilderFactory.get(), fieldNames);
    } else {
      prel = prel.postProcess(relBuilderFactory.get(), fieldNames);
    }

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
        Optional<AddedColumn> columnAdd = createColumnAddOp(env, op);
        columnAdd.ifPresent(c->addColumn(env, op, c, op.kind == IMPORT_TIMESTAMP));

        if (columnAdd.isEmpty() && op.statementKind == StatementKind.IMPORT) {
          setTimestampColumn(env, op);
        }

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
        break;
    }
  }

  private void setTimestampColumn(Env env, StatementOp op) {
    //get table from env
    ImportDefinition importDefinition = (ImportDefinition) op.getStatement();
    SQRLTable table = (SQRLTable) env.getRelSchema().getTable(importDefinition.getAlias().orElse(importDefinition.getImportPath().getLast())
            .getCanonical(),
        false)
        .getTable();

    QueryRelationalTable baseTbl = ((VirtualRelationalTable.Root) table.getVt()).getBase();
    Preconditions.checkState(importDefinition.getTimestamp().get() instanceof SqlIdentifier,
        "Aliased timestamps must use the AS keyword to set a new column");
    SqlIdentifier identifier = (SqlIdentifier)importDefinition.getTimestamp().get();
    RelDataTypeField field = baseTbl.getRowType().getField(identifier.names.get(0), false, false);

    baseTbl.getTimestamp()
        .getCandidateByIndex(field.getIndex())
        .fixAsTimestamp();
  }

  private void addColumn(Env env, StatementOp op, AddedColumn c, boolean fixTimestamp) {
    Optional<VirtualRelationalTable> optTable = getTargetRelTable(env, op);
    SQRLTable table = getContext(env, op.statement)
        .orElseThrow(() -> new RuntimeException("Cannot resolve table"));
    Name name = op.getStatement().getNamePath().getLast();
    Name vtName = uniquifyColumnName(name, table);

    c.setNameId(vtName.getCanonical());

    VirtualRelationalTable vtable = optTable.get();
    Optional<Integer> timestampScore = env.tableFactory.getTimestampScore(
        op.statement.getNamePath().getLast(), c.getDataType());
    Preconditions.checkState(vtable.getField(vtName) == null);
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

    table.addColumn(name, vtName, true, c.getDataType());
  }

  private Name uniquifyColumnName(Name name, SQRLTable table) {
    if (table.getField(name).isPresent()) {
      String newName = SqlValidatorUtil.uniquify(
          name.getCanonical(),
          new HashSet<>(table.getVt().getRowType().getFieldNames()),
          //Renamed columns to names the user cannot address to prevent collisions
          (original, attempt, size)-> original + "$" + attempt);
      return Name.system(newName);
    }

    return name;
  }

  private void updateJoinMapping(Env env, StatementOp op) {
    //op is a join, we need to discover the /to/ relationship
    Optional<SQRLTable> table = getContext(env, op.statement);
    JoinDeclarationUtil joinDeclarationUtil = new JoinDeclarationUtil(env);
    SQRLTable toTable =
        joinDeclarationUtil.getToTable(op.sqrlValidator,
            op.query);
    Multiplicity multiplicity = getMultiplicity(env, op);

    table.get().addRelationship(op.statement.getNamePath().getLast(), toTable,
        Relationship.JoinType.JOIN, multiplicity, Optional.of(op.joinDeclaration));
  }

  private Multiplicity getMultiplicity(Env env, StatementOp op) {
    Optional<SqlNode> fetch = getFetch(op);

    return fetch
        .filter(f -> ((SqlNumericLiteral) f).intValue(true) == 1)
        .map(f -> Multiplicity.ONE)
        .orElse(Multiplicity.MANY);
  }

  private Optional<SqlNode> getFetch(StatementOp op) {
    if (op.query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) op.query;
      return Optional.ofNullable(select.getFetch());
    } else if (op.query instanceof SqlOrderBy) {
      SqlOrderBy order = (SqlOrderBy) op.query;
      return Optional.ofNullable(order.fetch);
    } else {
      throw new RuntimeException("Unknown node type");
    }
  }

  private void createTable(Env env, StatementOp op, Optional<SQRLTable> parentTable) {

    final AnnotatedLP processedRel = convert(env, op);

    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> Name.system(n)).collect(Collectors.toList());
    Optional<Pair<SQRLTable, VirtualRelationalTable>> parentPair = parentTable.map(tbl ->
        Pair.of(tbl, env.tableMap.get(tbl)));

    ScriptTableDefinition queryTable;
    if (op.statementKind==StatementKind.STREAM) {
      StreamAssignment streamAssignment = (StreamAssignment) op.statement;
      queryTable=env.tableFactory.defineStreamTable(op.statement.getNamePath(), processedRel,
          StateChangeType.valueOf(streamAssignment.getType().name()),
              env.getSession().getPlanner().getRelBuilder(), env.getSession().getPipeline());
    } else {
      queryTable=env.tableFactory.defineTable(op.statement.getNamePath(), processedRel, fieldNames, parentPair);
    }

    registerScriptTable(env, queryTable);
  }


  private Optional<AddedColumn> createColumnAddOp(Env env, StatementOp op) {
    String columnName = op.statement.getNamePath().getLast().getCanonical();

    if (op.getStatementKind() == StatementKind.IMPORT) {
      ImportDefinition importDefinition = (ImportDefinition)op.getStatement();
      if (importDefinition.getTimestampAlias().isEmpty()) {
        return Optional.empty();
      }
    }

    //TODO: check for sub-queries
    if (isSimple(op)) {
      Project project = (Project) op.getRelNode();
      return Optional.of(new Simple(columnName, project.getProjects().get(project.getProjects().size() - 1)));
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

    SqrlJoinDeclarationSpec joinDeclaration;
    StatementOp(SqrlStatement statement, SqlNode query, StatementKind statementKind) {
      this.statement = statement;
      this.query = query;
      this.statementKind = statementKind;
    }
  }
}
