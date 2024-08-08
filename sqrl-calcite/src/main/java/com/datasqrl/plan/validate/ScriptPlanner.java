package com.datasqrl.plan.validate;

import static com.datasqrl.canonicalizer.ReservedName.SELF_IDENTIFIER;
import static com.datasqrl.canonicalizer.ReservedName.VARIABLE_PREFIX;
import static org.apache.calcite.sql.SqlUtil.stripAs;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.calcite.NormalizeTablePath;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.SqrlToSql;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlJoinBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.calcite.visitor.SqlTopLevelRelationVisitor;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.engine.log.LogFactory.Timestamp;
import com.datasqrl.engine.log.LogFactory.TimestampType;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.function.SqrlFunctionParameter.NoParameter;
import com.datasqrl.function.SqrlFunctionParameter.UnknownCaseParameter;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.TableSinkObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.CreateTableResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.local.generate.ResolvedExport;

import com.datasqrl.plan.table.RelDataTypeTableSchema;

import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.NestedRelationship;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.CheckUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlColumnDefinition;
import org.apache.calcite.sql.SqrlCreateDefinition;
import org.apache.calcite.sql.SqrlDistinctQuery;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlSqlQuery;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.SqrlTableParamDef;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlSqlValidator;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor(onConstructor_=@Inject)
@Getter
public class ScriptPlanner implements StatementVisitor<Void, Void> {
  private final SqrlFramework framework;
  private ModuleLoader moduleLoader;
  private ErrorCollector errorCollector;
  private final ExecutionGoal executionGoal;
  private final SqrlTableFactory tableFactory;
  private final PackageJson packageJson;

  private final SqlNameUtil nameUtil;
  private final ConnectorFactoryFactory connectorFactoryFactory;
  private final LogManager logManager;

  private final CreateTableResolver createTableResolver;
  private final Map<SqlNode, Object> schemaTable = new HashMap<>();
  private final AtomicInteger uniqueId = new AtomicInteger(0);
  private final Map<SqlNode, RelOptTable> tableMap = new HashMap<>();
  private final Map<FunctionParameter, SqlDynamicParam> paramMapping = new HashMap<>();
  private final Map<SqrlAssignment, SqlNode> preprocessSql = new HashMap<>();
  private final Map<SqrlAssignment, Boolean> isMaterializeTable = new HashMap<>();
  private final ArrayListMultimap<SqlNode, Function> isA = ArrayListMultimap.create();
  private final ArrayListMultimap<SqlNode, FunctionParameter> parameters = ArrayListMultimap.create();

  private static final String TEST_HINT_NAME = "test";

  @Override
  public Void visit(SqrlCreateDefinition statement, Void context) {
    NamePath namePath = NamePath.of(statement.getName().names.toArray(String[]::new));
    String logId = statement.getName().names.get(0);

    TypeFactory typeFactory = TypeFactory.getTypeFactory();
    RelDataTypeBuilder fieldBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    fieldBuilder.add(Name.system("_uuid"), typeFactory.createSqlType(SqlTypeName.VARCHAR));
    for (SqrlColumnDefinition definition : statement.getColumns()) {
      fieldBuilder.add(definition.getColumnName().getSimple(),
          SqlDataTypeSpecBuilder.create(definition.getType(), typeFactory));
    }

    RelDataType relDataType = fieldBuilder.build();

    createAndRegisterLogEntity(logId, namePath, relDataType);
    return null;
  }

  private Log createAndRegisterLogEntity(String logId, NamePath namePath, RelDataType relDataType) {
    Log sinkLog = logManager.create(logId, namePath.getLast(),
        relDataType, List.of("_uuid"), new Timestamp("event_time", TimestampType.LOG_TIME));

    relDataType = sinkLog.getSchema();

    TableConfig tableConfig = sinkLog.getSource().getConfiguration();
    this.framework.getSchema().add(new ResolvedImport(logId, tableConfig));

    NamespaceObject namespaceObject = createTableResolver.create(
        new TableSource(tableConfig, namePath, namePath.getLast(),
            new RelDataTypeTableSchema(relDataType)));
    namespaceObject.apply(this, Optional.empty(), framework, errorCollector);

    return sinkLog;
  }

  @Override
  public Void visit(SqrlImportDefinition node, Void context) {
    if (node.getImportPath().isStar() && node.getAlias().isPresent()) {
      throw addError(ErrorCode.IMPORT_CANNOT_BE_ALIASED, node, "Import cannot be aliased");
    }

    NamePath path = nameUtil.toNamePath(node.getImportPath().names);
    if (path.getFirst().getDisplay().equals("log")) {
      Log log = logManager.getLogs().get(path.getLast().getDisplay());
      NamespaceObject namespaceObject = createTableResolver.create(log.getSource());
      namespaceObject.apply(this, node.getAlias().map(SqlIdentifier::getSimple), framework, errorCollector);
    } else {
      SqrlModule module = moduleLoader.getModule(path.popLast()).orElse(null);

      if (module == null) {
        throw addError(ErrorCode.GENERIC, node, "Could not find module [%s] at path: [%s]",
            path, String.join("/", path.toStringList()));
      }

      if (node.getImportPath().isStar()) {
        if (module.getNamespaceObjects().isEmpty()) {
          addWarn(ErrorLabel.GENERIC, node, "Module is empty: %s", path);
        }

        module.getNamespaceObjects().forEach(obj -> obj.apply(this, Optional.empty(), framework, errorCollector));
      } else {
        Optional<NamespaceObject> namespaceObject = module.getNamespaceObject(path.getLast());
        namespaceObject.map(object -> object.apply(this, Optional.of(node.getAlias().map(a -> a.names.get(0))
                .orElse(/*retain alias*/path.getLast().getDisplay())), framework, errorCollector))
            .orElseThrow(() -> addError(ErrorCode.GENERIC, node, "Object [%s] not found in module: %s", path.getLast(), path));
      }
    }

    return null;
  }

  /**
   * Edge cases: LHS not a table RHS not a sink Resolve all field names for export
   */
  @Override
  public Void visit(SqrlExportDefinition node, Void context) {
    NamePath sinkPath = nameUtil.toNamePath(node.getSinkPath().names);

    NamePath path = nameUtil.toNamePath(node.getIdentifier().names);

    QueryPlanner planner = framework.getQueryPlanner();
    Optional<RelOptTable> table = planner.getCatalogReader().getTableFromPath(path);
    Preconditions.checkState(table.isPresent(), "Could not find export table: %s", path.getDisplay());
    ModifiableTable modTable = table.get().unwrap(ModifiableTable.class);

    int numSelects = modTable.getNumSelects();
    RelNode exportRelNode = planner.getRelBuilder().scan(modTable.getNameId()).build();

    Optional<SystemBuiltInConnectors> builtInSink = SystemBuiltInConnectors.forExport(sinkPath.popLast());
    TableSink tableSink = null;
    if (builtInSink.isPresent()) {
      SystemBuiltInConnectors connector = builtInSink.get();
      if (connector == SystemBuiltInConnectors.LOGGER) {
        String logger = packageJson.getCompilerConfig().getLogger();
        if (logger.equalsIgnoreCase("none")) {
          return null;
        } else if (logger.equalsIgnoreCase("print")) {
          connector = SystemBuiltInConnectors.PRINT_SINK;
        } else {
          if (!logger.equalsIgnoreCase(logManager.getEngineName())) {
            throw new IllegalStateException(String.format("The configured log engine is '%s'"
                    + " while 'compiler.logger' is set to '%s'. Please change the logger to"
                    + " the desired engine.",
                logManager.getEngineName(), logger));
          }
          Log sinkLog = logManager.create(modTable.getNameId(), sinkPath.getLast(),
              exportRelNode.getRowType(), List.of(), LogFactory.Timestamp.NONE);
          tableSink = sinkLog.getSink();
        }
      } else if (connector == SystemBuiltInConnectors.LOG_ENGINE) {
        Log sinkLog = logManager.getLogs().get(sinkPath.getLast().getDisplay());
        if (sinkLog == null) {
          sinkLog = createAndRegisterLogEntity(sinkPath.getLast().getDisplay(), sinkPath, exportRelNode.getRowType());
        }
        tableSink = sinkLog.getSink();
      }
      if (tableSink == null) {
        //Create the export for the built-in connector
        tableSink = connectorFactoryFactory.create(connector)
            .map(t -> t.createSourceAndSink(new ConnectorFactoryContext(sinkPath.getLast(),
                Map.of("id", modTable.getNameId(),
                    "name", sinkPath.getLast().getDisplay()))))
            .map(t -> TableSinkImpl.create(t, sinkPath.popLast(), Optional.empty())).get();
      }
    } else {
      Optional<TableSink> externalSink = moduleLoader.getModule(sinkPath.popLast())
          .flatMap(m -> m.getNamespaceObject(sinkPath.getLast()))
          .filter(t -> t instanceof TableSinkObject)
          .map(t -> ((TableSinkObject) t).getSink());
      if (externalSink.isEmpty()) {
        addError(ErrorCode.CANNOT_RESOLVE_TABLESINK, node, "Cannot resolve table sink: %s",
            nameUtil.toNamePath(node.getSinkPath().names).getDisplay());
        return null;
      }
      tableSink = externalSink.get();
    }
    ResolvedExport resolvedExport = new ResolvedExport(modTable.getNameId(), exportRelNode, numSelects, tableSink);
    framework.getSchema().add(resolvedExport);
    return null;
  }

  @Override
  public Void visit(SqrlAssignment assignment, Void context) {
    if (assignment.getIdentifier().names.size() > 1) {
      NamePath path = nameUtil.toNamePath(assignment.getIdentifier().names).popLast();
      Collection<Function> tableFunction = framework.getQueryPlanner().getSchema()
          .getFunctions(path.getDisplay(), false);
      if (tableFunction.isEmpty()) {
        throw addError(ErrorLabel.GENERIC, assignment.getIdentifier(), "Could not find table: %s",
            path.getDisplay());
      }
      Function function = Iterables.getOnlyElement(tableFunction);
      if (function instanceof Relationship
          && ((Relationship) function).getJoinType() != JoinType.CHILD) {
        addError(ErrorLabel.GENERIC, assignment.getIdentifier(), "Cannot assign query to table");
      }
      if (function instanceof NestedRelationship) {
        throw addError(ErrorLabel.GENERIC, assignment.getIdentifier(),
            "Cannot assign query to nested relationship");
      }

    }

    return null;
  }

  private boolean materializeSelfQuery(SqrlSqlQuery node) {
    if (node.getTableArgs().isEmpty()) {
      NamePath path = nameUtil.toNamePath(node.getIdentifier().names);
      Collection<Function> tableFunction = framework.getQueryPlanner().getSchema()
          .getFunctions(path.getDisplay(), false);
    }
    //don't materialize self if we have external arguments, the query will be inlined or called from gql
    return
        !(node.getTableArgs().isPresent() && node.getTableArgs().get().getParameters().size() > 0)
            ||
            //materialize self if we have a LIMIT clause
            (node.getQuery() instanceof SqlSelect
                && ((SqlSelect) node.getQuery()).getFetch() != null);
  }

  @Override
  public Void visit(SqrlExpressionQuery node, Void context) {

    //If on root table, use root table
    //If on nested, use @.table
    NamePath tablePath;
    if (node.getIdentifier().names.size() > 2) {
      Name last = nameUtil.toName(
          node.getIdentifier().names.get(node.getIdentifier().names.size() - 2));
      tablePath = ReservedName.SELF_IDENTIFIER.toNamePath().concat(last);
    } else if (node.getIdentifier().names.size() == 2) {
      tablePath = ReservedName.SELF_IDENTIFIER.toNamePath();
    } else {
      throw addError(ErrorLabel.GENERIC, node.getExpression(),
          "Cannot assign expression to root");
    }

    visit((SqrlAssignment) node, null);
    if (node.getTableArgs().isPresent()) {
      addError(ErrorLabel.GENERIC, node, "Table arguments for expressions not implemented yet.");
    }
    NamePath names = nameUtil.toNamePath(node.getIdentifier().names).popLast();
    Optional<RelOptTable> table = resolveModifiableTable(node, names);

    List<SqlNode> selectList = new ArrayList<>();
    selectList.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
        node.getExpression(),
        new SqlIdentifier(node.getIdentifier().names.get(node.getIdentifier().names.size() - 1),
            SqlParserPos.ZERO)));

    SqlSelect select = new SqlSelectBuilder()
        .setSelectList(selectList)
        .setFrom(new SqlIdentifier(tablePath.toStringList(), SqlParserPos.ZERO))
        .build();
    validateTable(node, select, node.getTableArgs(),
        true);

    isMaterializeTable.put(node, true);

    if (errorCollector.hasErrors()) {
      return null;
    }
//    NamePath path = nameUtil.toNamePath(node.getIdentifier().names).popLast();
//    RelOptTable table = planner.getCatalogReader().getTableFromPath(path);
    RexNode rexNode = framework.getQueryPlanner().planExpression(node.getExpression(), table.get().getRowType());
    addColumn(rexNode, Util.last(node.getIdentifier().names), table.get());

    return null;
  }

  @Override
  public Void visit(SqrlSqlQuery node, Void context) {
    boolean materializeSelf = materializeSelfQuery(node);
    isMaterializeTable.put(node, materializeSelf);

    visit((SqrlAssignment) node, null);
    validateTable(node, node.getQuery(), node.getTableArgs(), materializeSelf);

    postvisit(node, context);
    return null;
  }


  public Void postvisit(SqrlAssignment assignment, Void context) {
    if (errorCollector.hasErrors()) {
      return null;
    }
    QueryPlanner planner = framework.getQueryPlanner();
    SqlNode node = getPreprocessSql().get(assignment);
    boolean materializeSelf = getIsMaterializeTable().get(assignment);
    NamePath parentPath = nameUtil.getParentPath(assignment);
    NormalizeTablePath normalizeTablePath = new NormalizeTablePath(planner.getCatalogReader(),
        getParamMapping(), new SqlNameUtil(planner.getFramework().getNameCanonicalizer()), errorCollector);
    SqrlToSql sqrlToSql = new SqrlToSql(planner, planner.getCatalogReader(), planner.getOperatorTable(),
        normalizeTablePath, getParameters().get(assignment), framework.getSchema().getUniquePkId(), nameUtil);
    SqrlToSql.Result result = sqrlToSql.rewrite(node, materializeSelf, parentPath);
    this.isA.putAll(sqrlToSql.getIsA());
    validateTopLevelNamed(result.getSqlNode());

    RelNode relNode = planner.plan(Dialect.CALCITE, result.getSqlNode());
    RelNode expanded = planner.expandMacros(relNode);

    List<Function> isA = getIsA().get(node);

    if (assignment.getTableArgs().isEmpty()) {
      NamePath path = nameUtil.toNamePath(assignment.getIdentifier().names);
      planner.getSchema().clearFunctions(path);
    }

    if (assignment instanceof SqrlJoinQuery) {
      List<SqrlTableMacro> isASqrl = isA.stream()
          .map(f->((SqrlTableMacro)f))
          .collect(Collectors.toList());
      NamePath path = nameUtil.toNamePath(assignment.getIdentifier().names);
      SqrlTableMacro toTableMacro = isASqrl.get(isASqrl.size() - 1);
      if (toTableMacro instanceof NestedRelationship) {
        throw addError(ErrorLabel.GENERIC, assignment.getIdentifier(),
            "Cannot point join declaration to nested relationship.");
      }

      NamePath toTable = toTableMacro.getAbsolutePath();
      Supplier<RelNode> nodeSupplier = ()->expanded;

      Relationship rel = new Relationship(path.getLast(),
          path, toTable, Relationship.JoinType.JOIN, Multiplicity.MANY,
          result.getParams(), nodeSupplier, hasTestHint(assignment.getHints()));
      planner.getSchema().addRelationship(rel);
    } else {
      List<String> path = assignment.getIdentifier().names;
      RelNode rel = expanded;

      Optional<Supplier<RelNode>> nodeSupplier = result.getParams().isEmpty()
          ? Optional.empty()
          : Optional.of(()->rel);

      tableFactory.createTable(moduleLoader, path, rel, null,
          assignment.getHints(), result.getParams(), isA,
          materializeSelf, nodeSupplier, errorCollector, hasTestHint(assignment.getHints()));
    }

    return null;
  }

  /**
   * If nested, has a '@' table as the first table Validate args
   * <p>
   * Find all SELECT * on direct tables, these are possible Aliases.
   * <p>
   * Assure columns are named and no collisions.
   * <p>
   * We need a global PK incrementer to allow for multiple select * and have it still work (UUID
   * collisions)
   * <p>
   * If it errors completely: Derive type field names, give ANY
   * <p>
   * Allow all function resolution during validation, no need to plan, give any type
   * <p>
   * Allow UNION access tables
   */
  public void validateTable(SqrlAssignment statement, SqlNode query,
      Optional<SqrlTableFunctionDef> tableArgs, boolean materializeSelf) {
    QueryPlanner planner = framework.getQueryPlanner();
    SqlValidator validator = planner.createSqlValidator();

    Optional<SqrlTableMacro> parentTable = Optional.empty();
    if (statement.getIdentifier().names.size() > 1) {
      validateHasNestedSelf(query);
      NamePath parent = nameUtil.getParentPath(statement);
      Collection<Function> sqrlTable = planner.getSchema()
          .getFunctions(parent.getDisplay(), false);
      parentTable = Optional.of((SqrlTableMacro)Iterables.getOnlyElement(sqrlTable));
    }

    query = CalciteFixes.pushDownOrder(query);

    Pair<List<FunctionParameter>, SqlNode> argResult = transformArgs(parentTable, query, materializeSelf,
        tableArgs.orElseGet(() -> new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())));
    query = argResult.getRight();
    this.parameters.putAll(statement, argResult.getLeft());

    preprocessSql.put(statement, query);

    NamePath parent;
    if (statement.getIdentifier().names.size() > 1) {
      parent = nameUtil.getParentPath(statement);
      Collection<Function> sqrlTable = planner.getSchema()
          .getFunctions(parent.getDisplay(), false);
      if (sqrlTable.isEmpty()) {
        throw addError(ErrorLabel.GENERIC, statement.getIdentifier()
                .getComponent(statement.getIdentifier().names.size() - 1),
            "Could not find parent assignment table: %s",
            parent.getDisplay());
      }
    }

    if (statement instanceof SqrlExpressionQuery) {
      SqlNode aggregate = ((SqrlSqlValidator) validator).getAggregate((SqlSelect) query);
      if (aggregate != null) {
        throw addError(ErrorLabel.GENERIC, aggregate,
            "Aggregate functions not yet allowed");
      }
    }
  }

  private boolean validateTopLevelNamed(SqlNode sqlNode) {
    return SqlNodeVisitor.accept(new SqlTopLevelRelationVisitor<Boolean, Object>() {

      @Override
      public Boolean visitQuerySpecification(SqlSelect select, Object context) {
        boolean isValid = true;
        for (SqlNode node : select.getSelectList()) {
          isValid &= validSelectName(node);
        }
        return isValid;
      }

      private boolean validSelectName(SqlNode node) {
        if (node.getKind() != SqlKind.AS && node.getKind() != SqlKind.IDENTIFIER) {
          addError(ErrorLabel.GENERIC, node,
              "Selected column is missing a name. Try using the AS keyword.");
          return false;
        }
        return true;
      }

      @Override
      public Boolean visitOrderedUnion(SqlOrderBy node, Object context) {
        return SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);
      }

      @Override
      public Boolean visitSetOperation(SqlCall node, Object context) {
        for (SqlNode operator : node.getOperandList()) {
          boolean isValid = validateTopLevelNamed(operator);
          //only show one arm of unnamed columns
          if (!isValid) {
            return false;
          }
        }
        return true;
      }
    }, sqlNode, null);
  }

  private void validateHasNestedSelf(SqlNode query) {
    if (query.getKind() == SqlKind.UNION) {
      throw addError(ErrorLabel.GENERIC, query, "Nested unions not yet supported");
    } else if (query.getKind() != SqlKind.SELECT) {
      throw addError(ErrorLabel.GENERIC, query,
          "Unknown nested query type. Must be SELECT or JOIN");
    }
    SqlSelect select = (SqlSelect) query;
    SqlNode lhs = select.getFrom();
    lhs = stripAs(lhs);
    while (lhs instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) lhs;
      lhs = join.getLeft();
      lhs = stripAs(lhs);
    }
    if (!(lhs instanceof SqlIdentifier)) {
      throw addError(ErrorLabel.GENERIC, lhs, "Must be a table reference that starts with '@'");
    }
    SqlIdentifier identifier = (SqlIdentifier) lhs;
    if (!identifier.names.get(0).equals(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      throw addError(ErrorLabel.GENERIC, lhs, "Table must start with with '@'");
    }
  }

  public static boolean isSelfTable(SqlNode sqlNode) {
    if (sqlNode instanceof SqlCall &&
        ((SqlCall) sqlNode).getOperandList().get(0) instanceof SqlIdentifier) {
      SqlIdentifier id = ((SqlIdentifier) ((SqlCall) sqlNode).getOperandList()
          .get(0));
      return id.names.size() == 1 &&
          id.names.get(0).equalsIgnoreCase("@");
    }
    return false;
  }

  private Pair<List<FunctionParameter>, SqlNode> transformArgs(Optional<SqrlTableMacro> parentTable,
      SqlNode query,
      boolean materializeSelf, SqrlTableFunctionDef sqrlTableFunctionDef) {
    List<FunctionParameter> parameterList = toParams(sqrlTableFunctionDef.getParameters(),
        framework.getQueryPlanner().createSqlValidator());

    SqlNode node = SqlNodeVisitor.accept(new SqlRelationVisitor<>() {

      @Override
      public SqlNode visitQuerySpecification(SqlSelect node, Object context) {
        return new SqlSelectBuilder(node)
            .setFrom(SqlNodeVisitor.accept(this, node.getFrom(), null))
            .rewriteExpressions(rewriteVariables(parentTable, parameterList, materializeSelf))
            .build();
      }

      @Override
      public SqlNode visitAliasedRelation(SqlCall node, Object context) {
        List<SqlNode> ops = new ArrayList<>();
        ops.add(SqlNodeVisitor.accept(this, node.getOperandList().get(0), null));
        ops.addAll(node.getOperandList().subList(1, node.getOperandList().size()));
        return node.getOperator().createCall(node.getParserPosition(),
            ops.toArray(SqlNode[]::new));
      }

      @Override
      public SqlNode visitTable(SqlIdentifier node, Object context) {
        return node;
      }

      @Override
      public SqlNode visitJoin(SqlJoin node, Object context) {
        return new SqlJoinBuilder(node)
            .setLeft(SqlNodeVisitor.accept(this, node.getLeft(), null))
            .setRight(SqlNodeVisitor.accept(this, node.getRight(), null))
            .rewriteExpressions(rewriteVariables(parentTable, parameterList, materializeSelf))
            .build();
      }

      @Override
      public SqlNode visitSetOperation(SqlCall node, Object context) {
        return node.getOperator().createCall(node.getParserPosition(),
            node.getOperandList().stream()
                .map(o -> SqlNodeVisitor.accept(this, o, context))
                .collect(Collectors.toList()));
      }

      @Override
      public SqlNode visitCollectTableFunction(SqlCall node, Object context) {
        return visitAugmentedTable(node, context);
      }

      @Override
      public SqlNode visitLateralFunction(SqlCall node, Object context) {
        return visitAugmentedTable(node, context);
      }

      @Override
      public SqlNode visitUnnestFunction(SqlCall node, Object context) {
        return visitAugmentedTable(node, context);
      }

      private SqlNode visitAugmentedTable(SqlCall node, Object context) {
        SqlNode op = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);
        return node.getOperator().createCall(node.getFunctionQuantifier(), node.getParserPosition(), op);
      }

      @Override
      public SqlNode visitUserDefinedTableFunction(SqlCall node, Object context) {
        List<SqlNode> operands = node.getOperandList().stream()
            .map(f -> f.accept(rewriteVariables(parentTable, parameterList, materializeSelf)))
            .collect(Collectors.toList());
        return node.getOperator().createCall(node.getParserPosition(), operands);
      }

      @Override
      public SqlNode visitValues(SqlCall node, Object context) {
        return node;
      }

      @Override
      public SqlNode visitRow(SqlCall node, Object context) {
        return node;
      }

      @Override
      public SqlNode visitOrderedUnion(SqlOrderBy node, Object context) {
        SqlNode query = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);
        return node.getOperator().createCall(node.getParserPosition(),
            query, node.getOperandList().get(1), node.getOperandList().get(2), node.getOperandList().get(3));
      }



      @Override
      public SqlNode visitCall(SqlCall node, Object context) {
        throw addError(ErrorLabel.GENERIC, node, "Unsupported call: %s",
            node.getOperator().getName());
      }
    }, query, null);

    return Pair.of(parameterList, node);

  }

  public SqlShuttle rewriteVariables(Optional<SqrlTableMacro> parentTable, List<FunctionParameter> parameterList,
      boolean materializeSelf) {
    return new SqlShuttle() {
      @Override
      public SqlNode visit(SqlIdentifier id) {
        if (isSelfField(nameUtil.toNamePath(id.names)) && !materializeSelf) {
          //Add to param list if not there
          String name = id.names.get(1);
          for (FunctionParameter p : parameterList) {
            SqrlFunctionParameter s = (SqrlFunctionParameter) p;
            if (s.isInternal() && s.getName().equalsIgnoreCase(name)) {
              //already exists, return dynamic param of index
              if (paramMapping.get(p) != null) {
                return paramMapping.get(p);
              } else {
                throw new RuntimeException("unknown param");
              }
            }
          }

          //get the type from the current context
          if (parentTable.isEmpty()) {
            throw addError(ErrorLabel.GENERIC, id, "Cannot derive argument on root table");
          }
          RelDataTypeField field = framework.getQueryPlanner().getCatalogReader().nameMatcher().field(
              parentTable.get().getRowType(), name);
          if (field == null) {
            throw addError(ErrorLabel.GENERIC, id, "Cannot find field on parent table");
          }
          SqrlFunctionParameter functionParameter = new SqrlFunctionParameter(name,
              Optional.empty(), SqlDataTypeSpecBuilder
              .create(field.getType()), parameterList.size(), field.getType(),
              true, new UnknownCaseParameter(name));
          parameterList.add(functionParameter);
          SqlDynamicParam param = new SqlDynamicParam(functionParameter.getOrdinal(),
              id.getParserPosition());
          paramMapping.put(functionParameter, param);

          return param;
        } else if (isVariable(nameUtil.toNamePath(id.names))) {
          if (id.names.size() > 1) {
            addError(ErrorLabel.GENERIC, id, "Nested variables not yet implemented");
            return id;
          }

          List<FunctionParameter> defs = parameterList.stream()
              .filter(f -> f.getName().equalsIgnoreCase(id.getSimple()))
              .collect(Collectors.toList());

          if (defs.size() > 1) {
            throw addError(ErrorLabel.GENERIC, id, "Too many matching table arguments");
          }

          if (defs.size() != 1) {
            addError(ErrorLabel.GENERIC, id, "Could not find matching table arguments");
            return new SqlDynamicParam(0, id.getParserPosition());
          }

          FunctionParameter param = defs.get(0);

          if (paramMapping.get(param) != null) {
            return paramMapping.get(param);
          }

          int index = param.getOrdinal();

          SqlDynamicParam p = new SqlDynamicParam(index, id.getParserPosition());

          paramMapping.put(param, p);

          return p;
        }

        return super.visit(id);
      }


    };
  }

  public static boolean isVariable(NamePath names) {
    return names.get(0).hasPrefix(VARIABLE_PREFIX) && names.get(0).length() > 1;
  }

  public static boolean isSelfField(NamePath names) {
    return names.get(0).equals(SELF_IDENTIFIER) && names.size() > 1;
  }

  /**
   * if JOIN, it should be nested. if FROM, can be anywhere
   * <p>
   * Append an alias.* to the end so we can use the same logic to determine what it points to.
   */
  @Override
  public Void visit(SqrlJoinQuery node, Void context) {
    visit((SqrlAssignment) node, null);
    boolean materializeSelf = node.getQuery().getFetch() != null;
    isMaterializeTable.put(node, materializeSelf);
    checkAssignable(node);
    NamePath path = nameUtil.toNamePath(node.getIdentifier().names);
    NamePath parent = path.popLast();
    if (parent.isEmpty()) {
      throw addError(ErrorLabel.GENERIC, node.getIdentifier(),
          "Cannot assign join declaration on root");
    }

    /**
     * We want a SELECT lastAlias.* FROM ~ so query has proper number of fields.
     */
    Optional<String> lastAlias = extractLastAlias(node.getQuery().getFrom());
    if (lastAlias.isEmpty()) {
      throw addError(ErrorLabel.GENERIC, node.getQuery(), "Not a valid join declaration. "
          + "Could not derive table/alias of last join path item.");
    }

    SqlSelect select = new SqlSelectBuilder(node.getQuery())
        .setSelectList(List.of(new SqlIdentifier(List.of(lastAlias.get(), ""), SqlParserPos.ZERO)))
        .build();

    preprocessSql.put(node, select);
    validateTable(node, select, node.getTableArgs(), materializeSelf);
    postvisit(node, context);

    return null;
  }

  private Optional<String> extractLastAlias(SqlNode from) {
    return from.accept(new SqlBasicVisitor<>() {
      @Override
      public Optional<String> visit(SqlLiteral literal) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlCall call) {
        if (call.getKind() == SqlKind.JOIN) {
          return ((SqlJoin) call).getRight().accept(this);
        } else if (call.getKind() == SqlKind.AS) {
          return call.getOperandList().get(1).accept(this);
        }

        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlNodeList nodeList) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlIdentifier id) {
        if (id.isSimple()) {
          return Optional.of(id.getSimple());
        }
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlDataTypeSpec type) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlDynamicParam param) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlIntervalQualifier intervalQualifier) {
        return Optional.empty();
      }
    });
  }

  private void checkAssignable(SqrlAssignment node) {
    NamePath path = nameUtil.toNamePath(node.getIdentifier().names).popLast();
    if (path.isEmpty()) {
      return;
    }

    Collection<Function> tableFunction = framework.getQueryPlanner().getSchema().getFunctions(path.getDisplay(), false);
    if (tableFunction.isEmpty()) {
      addError(ErrorLabel.GENERIC, node, "Cannot column or query to table");
    }
  }

  @Override
  public Void visit(SqrlFromQuery node, Void context) {
    isMaterializeTable.put(node, false);

    visit((SqrlAssignment) node, null);
    if (node.getIdentifier().names.size() > 1) {
      addError(ErrorLabel.GENERIC, node.getIdentifier(),
          "FROM clause cannot be nested. Use JOIN instead.");
    }

    validateTable(node, node.getQuery(), node.getTableArgs(), false);


    if (errorCollector.hasErrors()) {
      return null;
    }

    postvisit(node, context);
    return null;
  }


  /**
   * Validate as query, cannot be nested (probably can be but disallow). No joins.
   * <p>
   * Must have Order by statement
   */
  @Override
  public Void visit(SqrlDistinctQuery node, Void context) {
    isMaterializeTable.put(node, true);
    if (node.getSelect().getOrderList() == null) {
      addError(ErrorLabel.GENERIC, node, "Order by statement must be specified");
    }

    if (node.getIdentifier().names.size() > 1) {
      addError(ErrorLabel.GENERIC, node, "Order by cannot be nested");
      return null;
    }

    preprocessSql.put(node, node.getSelect());

    validateTable(node, node.getSelect(), Optional.empty(), false);

    postvisit(node, context);

    return null;
  }

  private int addColumn(RexNode node, String cName, RelOptTable table) {
    if (table.unwrap(ModifiableTable.class) != null) {
      ModifiableTable table1 = (ModifiableTable) table.unwrap(Table.class);
      return table1.addColumn(cName, node, framework.getTypeFactory());
    } else {
      throw new RuntimeException();
    }
  }

  private Optional<RelOptTable> resolveModifiableTable(SqlNode node, NamePath names) {
    Optional<RelOptTable> table = framework.getCatalogReader().getTableFromPath(names);
    if (table.isEmpty()) {
      addError(ErrorLabel.GENERIC, node, "Could not find table: %s", names.getDisplay());
    }

    table.ifPresent((t) -> this.tableMap.put(node, t));

    table.ifPresent((t) -> {
      ModifiableTable modTable = t.unwrap(ModifiableTable.class);
      if (modTable == null) {
        addError(ErrorLabel.GENERIC, node, "Table cannot have a column added: %s",
            names.getDisplay());
      } else if (modTable.isLocked()) {
        addError(ErrorCode.TABLE_LOCKED, node, "Cannot add column to locked table: %s",
            names.getDisplay());
      }
    });

    return table;
  }

  private static List<FunctionParameter> toParams(List<SqrlTableParamDef> params,
      SqlValidator validator) {
    List<FunctionParameter> parameters = params.stream()
        .map(p -> new SqrlFunctionParameter(p.getName().getSimple(), p.getDefaultValue(),
            p.getType(), p.getIndex(), p.getType().deriveType(validator), false, new NoParameter()))
        .collect(Collectors.toList());
    return parameters;
  }

  public RuntimeException addError(ErrorLabel errorCode, CalciteContextException e) {
    RuntimeException exception = CheckUtil.createAstException(Optional.of(e), errorCode,
        () -> new SqlParserPos(e.getPosLine(), e.getPosColumn(), e.getEndPosLine(),
            e.getEndPosColumn()),
        e::getMessage);
    return errorCollector.handle(exception);
  }

  public static RuntimeException addError(ErrorCollector errorCollector, ErrorLabel errorCode,
      SqlNode node,
      String message, Object... format) {
    RuntimeException exception = CheckUtil.createAstException(errorCode, node,
        format == null ? message : String.format(message, format));
    return errorCollector.handle(exception);
  }

  public RuntimeException addError(Throwable cause, ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    RuntimeException exception = CheckUtil.createAstException(Optional.of(cause), errorCode,
        node::getParserPosition,
        () -> format == null || message == null ? message : String.format(message, format));
    return errorCollector.handle(exception);
  }

  private RuntimeException addError(ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    return addError(errorCollector, errorCode, node, message, format);
  }

  private void addWarn(ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    //todo: warn
    addError(errorCode, node, message, format);
  }

  public void plan(MainScript mainScript, ModuleLoader composite) {
    ErrorCollector errors = errorCollector;
    if (errorCollector.getLocation() == null
        || errorCollector.getLocation().getSourceMap() == null) {
      errors = errorCollector.withSchema("<schema>", mainScript.getContent());
    }

    ScriptNode scriptNode;
    try {
      scriptNode = (ScriptNode) framework.getQueryPlanner().parse(Dialect.SQRL,
          mainScript.getContent());
    } catch (Exception e) {
      throw errors.handle(e);
    }

    ErrorCollector scriptErrors = errorCollector.withScript("<script>", mainScript.getContent());

    for (SqlNode statement : scriptNode.getStatements()) {
      try {
        ErrorCollector lineErrors = scriptErrors
            .atFile(SqrlAstException.toLocation(statement.getParserPosition()));
        errorCollector = lineErrors;
        moduleLoader = composite;
        SqrlStatement stmt = (SqrlStatement) statement;
        if (hasTestHint(stmt.getHints()) && executionGoal != ExecutionGoal.TEST) {
          //Skip test annotations
          continue;
        }

        stmt.accept(this, null);
        if (lineErrors.hasErrors()) {
          throw new CollectedException(new RuntimeException("Script cannot validate"));
        }
      } catch (CollectedException e) {
        throw e;
      } catch (Exception e) {
        //Print stack trace for unknown exceptions
        if (e.getMessage() == null || e instanceof IllegalStateException
            || e instanceof NullPointerException) {
          e.printStackTrace();
        }

        ErrorCollector statementErrors = scriptErrors
            .atFile(SqrlAstException.toLocation(statement.getParserPosition()));

        throw statementErrors.handle(e);
      }
    }
  }

  private boolean hasTestHint(Optional<SqlNodeList> optionalStmt) {
    return optionalStmt.isPresent() && optionalStmt.get().getList().stream()
        .anyMatch(node -> node instanceof SqlHint && TEST_HINT_NAME.equalsIgnoreCase(((SqlHint) node).getName()));
  }
}
