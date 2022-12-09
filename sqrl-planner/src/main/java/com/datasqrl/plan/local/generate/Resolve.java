/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.generate;

import static com.datasqrl.error.ErrorCode.IMPORT_CANNOT_BE_ALIASED;
import static com.datasqrl.error.ErrorCode.IMPORT_STAR_CANNOT_HAVE_TIMESTAMP;
import static com.datasqrl.plan.local.generate.Resolve.OpKind.IMPORT_TIMESTAMP;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.error.SourceMapImpl;
import com.datasqrl.function.builtin.time.FlinkFnc;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.DynamicExporter;
import com.datasqrl.loaders.DynamicLoader;
import com.datasqrl.loaders.Exporter;
import com.datasqrl.loaders.Loader;
import com.datasqrl.loaders.LoaderContext;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.name.ReservedName;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.calcite.OptimizationStage;
import com.datasqrl.plan.calcite.SqlValidatorUtil;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.calcite.hints.ExecutionHint;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import com.datasqrl.plan.calcite.table.AbstractRelationalTable;
import com.datasqrl.plan.calcite.table.AddedColumn;
import com.datasqrl.plan.calcite.table.AddedColumn.Simple;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.datasqrl.plan.calcite.table.ProxySourceRelationalTable;
import com.datasqrl.plan.calcite.table.QueryRelationalTable;
import com.datasqrl.plan.calcite.table.StateChangeType;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.local.transpile.AddContextFields;
import com.datasqrl.plan.local.transpile.AddContextTable;
import com.datasqrl.plan.local.transpile.AddHints;
import com.datasqrl.plan.local.transpile.AllowMixedFieldUnions;
import com.datasqrl.plan.local.transpile.AnalyzeStatement;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.plan.local.transpile.ConvertJoinDeclaration;
import com.datasqrl.plan.local.transpile.FlattenFieldPaths;
import com.datasqrl.plan.local.transpile.FlattenTablePaths;
import com.datasqrl.plan.local.transpile.JoinDeclarationUtil;
import com.datasqrl.plan.local.transpile.QualifyIdentifiers;
import com.datasqrl.plan.local.transpile.ReplaceWithVirtualTable;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.Assignment;
import org.apache.calcite.sql.CreateSubscription;
import org.apache.calcite.sql.DistinctAssignment;
import org.apache.calcite.sql.ExportDefinition;
import org.apache.calcite.sql.ExpressionAssignment;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.JoinAssignment;
import org.apache.calcite.sql.QueryAssignment;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.StreamAssignment;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

@Getter
@Slf4j
public class Resolve {

  private final Path basePath;

  public Resolve(Path basePath) {
    this.basePath = basePath;
  }

  @Getter
  public class Env {

    CalciteTableFactory tableFactory = new CalciteTableFactory(TypeFactory.getTypeFactory());
    SchemaAdjustmentSettings schemaAdjustmentSettings = SchemaAdjustmentSettings.DEFAULT;
    Loader loader = new DynamicLoader();
    Exporter exporter = new DynamicExporter();
    List<StatementOp> ops = new ArrayList<>();
    List<SqrlStatement> queryOperations = new ArrayList<>();
    NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM;

    // Mappings
    Map<SQRLTable, VirtualRelationalTable> tableMap = new HashMap<>();
    SqrlCalciteSchema userSchema;
    SqrlCalciteSchema relSchema;
    List<ResolvedExport> exports = new ArrayList<>();

    Session session;
    ScriptNode scriptNode;
    Path packagePath;
    ExecutionPipeline pipeline;

    //Updated while processing each node
    SqlNode currentNode = null;
    ErrorCollector errors;

    List<FlinkFnc> resolvedFunctions = new ArrayList<>();

    public Env(SqrlCalciteSchema relSchema, Session session,
        ScriptNode scriptNode, Path packagePath, ErrorCollector errorCollector) {
      this.relSchema = relSchema;
      this.session = session;
      this.scriptNode = scriptNode;
      this.packagePath = packagePath;
      this.errors = errorCollector;
    }


    public Name registerTable(TableSource tableSource, Optional<Name> alias) {
      ScriptTableDefinition def = getTableFactory().importTable(tableSource, alias,
          getSession().getPlanner().getRelBuilder(), getSession().getPipeline());

      checkState(
          getRelSchema().getTable(def.getTable().getName().getCanonical(), false) == null,
          ErrorCode.IMPORT_NAMESPACE_CONFLICT,
          () -> getCurrentNode().getParserPosition(),
          () -> String.format("An item named `%s` is already in scope",
              def.getTable().getName().getDisplay()));

      registerScriptTable(this, def);
      return tableSource.getName();
    }

    public Resolve getResolve() {
      return Resolve.this;
    }
  }

  public Env planDag(Session session, ScriptNode script) {
    Env env = createEnv(session, script, script.getOriginalScript());

    try {
      resolveImports(env);
      mapOperations(env);
      planQueries(env);
      return env;
    } catch (Exception e) {
      env.errors.handle(e);
      throw e;
    }
  }

  public Env createEnv(Session session, ScriptNode script, String originalScript) {
    // All operations are applied to this env
    return new Env(
        session.planner.getDefaultSchema().unwrap(SqrlCalciteSchema.class),
        session,
        script,
        basePath,
        session.getErrors()
            .fromPrefix(ErrorPrefix.SCRIPT)
            .sourceMap(new SourceMapImpl(originalScript))
    );
  }

  private void validateImportInHeader(Env env) {
    boolean inHeader = true;
    for (SqlNode statement : env.scriptNode.getStatements()) {
      if (statement instanceof ImportDefinition) {
        checkState(inHeader, ErrorCode.IMPORT_IN_HEADER, statement::getParserPosition,
            () -> "Import statements must be in header");
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
    NamePath fullPath = toNamePath(env, node.getImportPath());
    Name last = fullPath.getLast();
    NamePath basePath = fullPath.popLast();
    /*
     * Add all files to namespace
     */
    LoaderContext context = new LoaderContextImpl(env);
    if (last.equals(ReservedName.ALL)) {
      checkState(node.getAlias().isEmpty(), IMPORT_CANNOT_BE_ALIASED,
          () -> node.getAlias().get().getParserPosition(),
          () -> "Alias `" + node.getAlias().get().names.get(0) + "` cannot be used here.");

      checkState(node.getTimestamp().isEmpty(), IMPORT_STAR_CANNOT_HAVE_TIMESTAMP,
          () -> node.getTimestamp().get().getParserPosition(),
          () -> "Cannot use timestamp with import star");

      Collection<Name> loaded = env.getLoader().loadAll(context, basePath);
      Preconditions.checkState(!loaded.isEmpty(),
          "Import [%s] is not a package or package is empty", basePath);
    } else {
      boolean loaded = env.getLoader().load(context, fullPath, node.getAlias()
          .map(a -> toNamePath(env, a).getFirst()));
      Preconditions.checkState(loaded, "Could not import: %s", fullPath);
    }
  }

  private String pathToString(SqlIdentifier id) {
    return String.join(".", id.names);
  }

  private NamePath toNamePath(Env env, SqlIdentifier identifier) {
    return toNamePath(env.canonicalizer, identifier);
  }

  public static NamePath toNamePath(NameCanonicalizer canonicalizer, SqlIdentifier identifier) {
    return NamePath.of(identifier.names.stream()
        .map(i -> (i.equals(""))
            ? ReservedName.ALL
            : canonicalizer.name(i)
        )
        .collect(Collectors.toList())
    );
  }

  private void setCurrentNode(Env env, SqrlStatement node) {
    env.currentNode = node;
    env.errors = env.errors.resolve(pathToString(node.getNamePath()));
  }

  public void registerScriptTable(Env env, ScriptTableDefinition tblDef) {
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
      env.relSchema.add(tblDef.getTable().getName().getDisplay(), (org.apache.calcite.schema.Table)
          tblDef.getTable());
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
      return OpKind.JOIN;
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

    } else if (statement instanceof ExportDefinition) {
      //We handle exports out-of-band and just resolve them.
      ExportDefinition export = (ExportDefinition) statement;
      Optional<SQRLTable> tableOpt = resolveTable(env, toNamePath(env, export.getTablePath()),
          false);
      Optional<TableSink> sink = env.getExporter()
          .export(new LoaderContextImpl(env), toNamePath(env, export.getSinkPath()));
      checkState(tableOpt.isPresent(), ErrorCode.MISSING_DEST_TABLE, export::getParserPosition,
          () -> String.format("Could not find table path: %s", export.getTablePath()));
      Preconditions.checkArgument(sink.isPresent());
      SQRLTable table = tableOpt.get();
      Preconditions.checkArgument(table.getVt().getRoot().getBase().getExecution().isWrite());
      RelBuilder relBuilder = env.getSession().getPlanner().getRelBuilder()
          .scan(table.getVt().getNameId());
      List<RexNode> selects = new ArrayList<>();
      List<String> fieldNames = new ArrayList<>();
      table.getVisibleColumns().stream().forEach(c -> {
        selects.add(relBuilder.field(c.getShadowedName().getCanonical()));
        fieldNames.add(c.getName().getDisplay());
      });
      relBuilder.project(selects, fieldNames);
      env.exports.add(new ResolvedExport(table.getVt(), relBuilder.build(), sink.get()));
      return Optional.empty();
    } else {
      throw fatal(env, "Unrecognized assignment type: " + statement.getClass());
    }

    StatementOp op = new StatementOp(statement, sqlNode, statementKind);
    env.ops.add(op);
    return Optional.of(op);
  }

  private SqlNode transformExpressionToQuery(Env env, SqrlStatement statement, SqlNode sqlNode) {
    checkState(getContext(env, statement).isPresent(), ErrorCode.MISSING_DEST_TABLE, statement,
        String.format("Could not find table: %s", statement.getNamePath()));
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
    List<String> assignmentPath = toNamePath(env, op.getStatement().getNamePath()).popLast()
        .stream()
        .map(e -> e.getCanonical())
        .collect(Collectors.toList());

    Optional<SQRLTable> table = getContext(env, op.getStatement());
    table.ifPresent(
        t -> assurePathWritable(env, op, toNamePath(env, op.statement.getNamePath()).popLast()));
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
      log.trace("Transformed node: {}", node);
      currentAnalysis = analyzer.apply(node);
    }

    final SqlNode sql = node.accept(new ConvertJoinDeclaration(context));
    SqlValidator sqrlValidator = SqlValidatorUtil.createSqlValidator(env.relSchema,
        env.getResolvedFunctions()
    );

    sqrlValidator.validate(sql);

    if (op.getStatementKind() == StatementKind.DISTINCT_ON) {
      checkState(context.isEmpty(), ErrorCode.NESTED_DISTINCT_ON, op.getStatement());
      new AddHints(sqrlValidator, context).accept(op, sql);

      SqlValidator validate2 = SqlValidatorUtil.createSqlValidator(env.relSchema,
          env.getResolvedFunctions());
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

      SqlValidator prevalidate = SqlValidatorUtil.createSqlValidator(env.relSchema,
          env.getResolvedFunctions());
      prevalidate.validate(rewritten);
      new AddHints(prevalidate, context).accept(op, rewritten);

      SqlValidator validator = SqlValidatorUtil.createSqlValidator(env.relSchema,
          env.getResolvedFunctions());
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
    return resolveTable(env, toNamePath(env, statement.getNamePath()), true);
  }

  private Optional<SQRLTable> resolveTable(Env env, NamePath namePath, boolean getParent) {
    if (getParent && !namePath.isEmpty()) {
      namePath = namePath.popLast();
    }
    if (namePath.isEmpty()) {
      return Optional.empty();
    }
    Optional<SQRLTable> table =
        Optional.ofNullable(env.relSchema.getTable(namePath.get(0).getDisplay(), false))
            .map(t -> (SQRLTable) t.getTable());
    NamePath childPath = namePath.popFirst();
    return table.flatMap(t -> t.walkTable(childPath));
  }

  public void plan(Env env, StatementOp op) {
    env.session.planner.refresh();
    env.session.planner.setValidator(op.getQuery(), op.getSqrlValidator());

    RelNode relNode = env.session.planner.rel(op.getQuery()).rel;

    //Optimization prepass: TODO: why are we doing this here?
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
    if (op.statementKind == StatementKind.STREAM) {
      Preconditions.checkArgument(
          !execHint.filter(h -> h.getExecType() != ExecutionEngine.Type.STREAM).isPresent(),
          "Invalid execution hint: %s", execHint);
      if (execHint.isEmpty()) {
        execHint = Optional.of(new ExecutionHint(ExecutionEngine.Type.STREAM));
      }
    }
    execHint.map(h -> h.getConfig(env.session.getPipeline(), configBuilder));
    SQRLLogicalPlanConverter.Config config = configBuilder.build();
    if (config.getStartStage() != null) {
      prel = SQRLLogicalPlanConverter.convert(relNode, relBuilderFactory, config);
    } else {
      prel = SQRLLogicalPlanConverter.findCheapest(toNamePath(env, op.statement.getNamePath()),
          relNode, relBuilderFactory, env.session.pipeline, config);
    }
    if (op.statementKind == StatementKind.DISTINCT_ON) {
      //Get all field names from resulting relnode instead of the op
      fieldNames = prel.relNode.getRowType().getFieldNames();
    }
    if (op.statementKind == StatementKind.STREAM) {
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
        columnAdd.ifPresent(c -> addColumn(env, op, c, op.kind == IMPORT_TIMESTAMP));

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
        Optional<SQRLTable> tbl = getContext(env, op.getStatement());
        checkState(tbl.isPresent(), ErrorCode.GENERIC_ERROR, op.statement,
            "Root JOIN declarations are not yet supported");

        updateJoinMapping(env, op);
        break;
    }
  }

  private void setTimestampColumn(Env env, StatementOp op) {
    //get table from env
    ImportDefinition importDefinition = (ImportDefinition) op.getStatement();
    SQRLTable table = (SQRLTable) env.getRelSchema().getTable(importDefinition.getAlias()
                .map(i -> toNamePath(env, i).get(0))
                .orElse(toNamePath(env, importDefinition.getImportPath()).getLast())
                .getCanonical(),
            false)
        .getTable();

    QueryRelationalTable baseTbl = ((VirtualRelationalTable.Root) table.getVt()).getBase();
    Preconditions.checkState(importDefinition.getTimestamp().isPresent(),
        "Internal error: timestamp should be present");

    checkState(importDefinition.getTimestamp().get() instanceof SqlIdentifier,
        ErrorCode.TIMESTAMP_COLUMN_EXPRESSION, importDefinition.getTimestamp().get());
    SqlIdentifier identifier = (SqlIdentifier) importDefinition.getTimestamp().get();
    RelDataTypeField field = baseTbl.getRowType().getField(identifier.names.get(0), false, false);

    checkState(field != null,
        ErrorCode.TIMESTAMP_COLUMN_MISSING, importDefinition.getTimestamp().get());
    baseTbl.getTimestamp()
        .getCandidateByIndex(field.getIndex())
        .fixAsTimestamp();
  }

  private void addColumn(Env env, StatementOp op, AddedColumn c, boolean fixTimestamp) {
    Optional<VirtualRelationalTable> optTable = getTargetRelTable(env, op);
    SQRLTable table = getContext(env, op.statement)
        .orElseThrow(() -> new RuntimeException("Cannot resolve table"));
    if (table.getField(toNamePath(env, op.statement.getNamePath()).getLast()).isPresent()) {
      checkState(!(table.getField(toNamePath(env, op.statement.getNamePath()).getLast())
              .get() instanceof Relationship),
          ErrorCode.CANNOT_SHADOW_RELATIONSHIP, op.statement);
    }

    Name name = toNamePath(env, op.getStatement().getNamePath()).getLast();
    Name vtName = uniquifyColumnName(name, table);

    c.setNameId(vtName.getCanonical());

    VirtualRelationalTable vtable = optTable.get();
    Optional<Integer> timestampScore = env.tableFactory.getTimestampScore(
        toNamePath(env, op.statement.getNamePath()).getLast(), c.getDataType());
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
      String newName = org.apache.calcite.sql.validate.SqlValidatorUtil.uniquify(
          name.getCanonical(),
          new HashSet<>(table.getVt().getRowType().getFieldNames()),
          //Renamed columns to names the user cannot address to prevent collisions
          (original, attempt, size) -> original + "$" + attempt);
      return Name.system(newName);
    }

    return name;
  }

  private void updateJoinMapping(Env env, StatementOp op) {
    //op is a join, we need to discover the /to/ relationship
    SQRLTable table = getContext(env, op.statement)
        .orElseThrow(() -> new RuntimeException("Internal Error: Missing context"));
    JoinDeclarationUtil joinDeclarationUtil = new JoinDeclarationUtil(
        env.getSession().getPlanner().getRelBuilder().getRexBuilder());
    SQRLTable toTable =
        joinDeclarationUtil.getToTable(op.sqrlValidator,
            op.query);
    Multiplicity multiplicity = getMultiplicity(env, op);

    checkState(table.getField(toNamePath(env, op.statement.getNamePath()).getLast()).isEmpty(),
        ErrorCode.CANNOT_SHADOW_RELATIONSHIP, op.statement);

    table.addRelationship(toNamePath(env, op.statement.getNamePath()).getLast(), toTable,
        Relationship.JoinType.JOIN, multiplicity, Optional.of(op.joinDeclaration));
  }

  private Multiplicity getMultiplicity(Env env, StatementOp op) {
    Optional<SqlNode> fetch = getFetch(env, op);

    return fetch
        .filter(f -> ((SqlNumericLiteral) f).intValue(true) == 1)
        .map(f -> Multiplicity.ONE)
        .orElse(Multiplicity.MANY);
  }

  private Optional<SqlNode> getFetch(Env env, StatementOp op) {
    if (op.query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) op.query;
      return Optional.ofNullable(select.getFetch());
    } else if (op.query instanceof SqlOrderBy) {
      SqlOrderBy order = (SqlOrderBy) op.query;
      return Optional.ofNullable(order.fetch);
    } else {
      throw fatal(env, "Unknown node type");
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
    if (op.statementKind == StatementKind.STREAM) {
      StreamAssignment streamAssignment = (StreamAssignment) op.statement;
      queryTable = env.tableFactory.defineStreamTable(toNamePath(env, op.statement.getNamePath()),
          processedRel,
          StateChangeType.valueOf(streamAssignment.getType().name()),
          env.getSession().getPlanner().getRelBuilder(), env.getSession().getPipeline());
    } else {
      queryTable = env.tableFactory.defineTable(toNamePath(env, op.statement.getNamePath()),
          processedRel, fieldNames, parentPair);
    }

    registerScriptTable(env, queryTable);
  }


  private Optional<AddedColumn> createColumnAddOp(Env env, StatementOp op) {
    String columnName = toNamePath(env, op.statement.getNamePath()).getLast().getCanonical();

    if (op.getStatementKind() == StatementKind.IMPORT) {
      ImportDefinition importDefinition = (ImportDefinition) op.getStatement();
      if (importDefinition.getTimestampAlias().isEmpty()) {
        return Optional.empty();
      }
    }

    //TODO: check for sub-queries
    if (isSimple(op)) {
      Project project = (Project) op.getRelNode();
      return Optional.of(
          new Simple(columnName, project.getProjects().get(project.getProjects().size() - 1)));
    } else {
      //return new Complex(columnName, op.relNode);
      throw unsupportedOperation(env, "Complex column not yet supported");
    }
  }

  private void assurePathWritable(Env env, StatementOp op, NamePath path) {
    Optional<SQRLTable> table = Optional.ofNullable(
            env.relSchema.getTable(path.get(0).getCanonical(), false))
        .filter(e -> e.getTable() instanceof SQRLTable)
        .map(e -> (SQRLTable) e.getTable());
    Optional<Field> field = table
        .map(t -> t.walkField(path.popFirst()))
        .stream()
        .flatMap(Collection::stream)
        .filter(f -> f instanceof Relationship && (
            ((Relationship) f).getJoinType() == Relationship.JoinType.JOIN
                || ((Relationship) f).getJoinType() == Relationship.JoinType.PARENT))
        .findAny();
    checkState(field.isEmpty(), ErrorCode.PATH_CONTAINS_RELATIONSHIP, op.statement);
  }

  private boolean isSimple(StatementOp op) {
    RelNode relNode = op.getRelNode();
    return relNode instanceof Project && relNode.getInput(0) instanceof TableScan;
  }

  private Optional<VirtualRelationalTable> getTargetRelTable(Env env, StatementOp op) {
    Optional<SQRLTable> context = getContext(env, op.statement);
    return context.map(c -> env.getTableMap().get(c));
  }

  public void checkState(boolean check, ErrorCode code, SqlNode node) {
    checkState(check, code, node::getParserPosition, () -> "");
  }

  public void checkState(boolean check, ErrorCode code, SqlNode node, String message) {
    checkState(check, code, node::getParserPosition, () -> message);
  }

  public void checkState(boolean check, ErrorCode code,
      Supplier<SqlParserPos> pos, Supplier<String> message) {
    if (!check) {
      throw createAstException(code, pos, message);
    }
  }

  public RuntimeException createAstException(ErrorCode code, Supplier<SqlParserPos> pos,
      Supplier<String> message) {
    return new SqrlAstException(code, pos.get(), message.get());
  }

  private RuntimeException fatal(Env env, String message) {
    return createAstException(ErrorCode.GENERIC_ERROR,
        () -> env.getCurrentNode().getParserPosition(), () -> message);
  }

  private RuntimeException unsupportedOperation(Env env, String message) {
    return createAstException(ErrorCode.GENERIC_ERROR,
        () -> env.getCurrentNode().getParserPosition(), () -> message);
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

  @Value
  public static class ResolvedExport {

    VirtualRelationalTable table;
    RelNode relNode;
    TableSink sink;

  }

}
