/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner;

import static com.datasqrl.function.FlinkUdfNsObject.getFunctionNameFromClass;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.graphql.server.MutationComputedColumnType;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.planner.FlinkPhysicalPlan.Builder;
import com.datasqrl.planner.analyzer.SQRLLogicalPlanAnalyzer;
import com.datasqrl.planner.analyzer.SQRLLogicalPlanAnalyzer.ViewAnalysis;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.plan.MutationQuery.MutationQueryBuilder;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.parser.ParsePosUtil;
import com.datasqrl.planner.parser.ParsePosUtil.MessageLocation;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement.ParsedArgument;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.planner.tables.FlinkConnectorConfig;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.planner.tables.SourceSinkTableAnalysis;
import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.operations.SqlNodeConvertContext;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

/**
 * This class acts as the "translator" between the {@link SqlScriptPlanner} and the Flink parser and
 * planner (and, by extension, Calcite).
 *
 * <p>In its role as the translator, this class does a number of things:
 *
 * <ul>
 *   <li>Provide access to the Flink planner and it's components like the FlinkRelBuilder and other
 *       planner classes that we need access to. In some cases, we need to use hacky reflection to
 *       get access because they are private
 *   <li>Parse strings to SqlNodes, and convert SqlNodes to RelNodes. And also the inverse: Convert
 *       RelNodes to SqlNodes and unparse SqlNodes to strings.
 *   <li>Handle the additional parsing logic that SQRL introduces for function argument signatures,
 *       as well as creating views, parsing CREATE TABLE statements and such. For created views and
 *       tables, it invokes the {@link SQRLLogicalPlanAnalyzer} to extract the information needed
 *       for the DAG construction.
 *   <li>Keeps track of everything we add to Flink the builder for the {@link FlinkPhysicalPlan}.
 * </ul>
 */
public class Sqrl2FlinkSQLTranslator {

  public static final String SCHEMA_SUFFIX = "__schema";

  private final RuntimeExecutionMode executionMode;
  private final boolean compileFlinkPlan;
  private final StreamTableEnvironmentImpl tEnv;
  private final Supplier<FlinkPlannerImpl> validatorSupplier;
  private final SqrlFunctionCatalog sqrlFunctionCatalog;
  private final CatalogManager catalogManager;
  private final FlinkPhysicalPlan.Builder planBuilder;
  @Getter private final FlinkTypeFactory typeFactory;

  @Getter private final TableAnalysisLookup tableLookup = new TableAnalysisLookup();

  public Sqrl2FlinkSQLTranslator(
      BuildPath buildPath, FlinkStreamEngine flink, CompilerConfig compilerConfig) {
    this.executionMode = flink.getExecutionMode();
    this.compileFlinkPlan = compilerConfig.compileFlinkPlan();
    // Set up a StreamExecution Environment in Flink with configuration and access to jars
    var jarUrls = getUdfUrls(buildPath);
    // Create a UDF class loader and configure
    ClassLoader udfClassLoader =
        new URLClassLoader(jarUrls.toArray(new URL[0]), getClass().getClassLoader());

    // Init Flink config
    var config = flink.getBaseConfiguration();

    if (!jarUrls.isEmpty()) {
      config.set(
          PipelineOptions.CLASSPATHS,
          jarUrls.stream().map(URL::toString).collect(Collectors.toList()));
    }

    this.planBuilder = new Builder(config.clone());

    if (executionMode == RuntimeExecutionMode.STREAMING) {
      planBuilder.addInferredConfig(flink.getStreamingSpecificConfig());
    }

    // Set up table environment
    var sEnv = StreamExecutionEnvironment.getExecutionEnvironment(planBuilder.getConfig());
    var tEnvConfig =
        EnvironmentSettings.newInstance()
            .withConfiguration(planBuilder.getConfig())
            .withClassLoader(udfClassLoader)
            .build();
    this.tEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(sEnv, tEnvConfig);

    // Extract a number of classes we need access to for planning
    this.validatorSupplier = ((PlannerBase) tEnv.getPlanner())::createFlinkPlanner;
    var planner = this.validatorSupplier.get();
    typeFactory = (FlinkTypeFactory) planner.getOrCreateSqlValidator().getTypeFactory();
    // Initialize function catalog (custom)
    sqrlFunctionCatalog = new SqrlFunctionCatalog(typeFactory);
    var calciteConfigBuilder = new CalciteConfigBuilder();
    calciteConfigBuilder.addSqlOperatorTable(sqrlFunctionCatalog.getOperatorTable());
    //    setOptimizerRules(calciteConfigBuilder,tEnv.getConfig()); TODO: fix, so we have more
    // effective subgraph identification by not pushing down projections

    this.tEnv.getConfig().setPlannerConfig(calciteConfigBuilder.build());
    this.catalogManager = tEnv.getCatalogManager();

    // Register SQRL standard library functions
    ServiceLoader<AutoRegisterSystemFunction> standardLibraryFunctions =
        ServiceLoader.load(AutoRegisterSystemFunction.class);
    standardLibraryFunctions.forEach(
        fct ->
            this.addUserDefinedFunction(
                FlinkUdfNsObject.getFunctionNameFromClass(fct.getClass()).getDisplay(),
                fct.getClass().getName(),
                true));
  }

  public SqrlRexUtil getRexUtil() {
    return new SqrlRexUtil(typeFactory);
  }

  public SqlNode parseSQL(String sqlStatement) {
    CalciteParser parser;
    try {
      // TODO: This is a hack - is there a better way to get the calcite parser?
      var calciteSupplierField = ParserImpl.class.getDeclaredField("calciteParserSupplier");
      calciteSupplierField.setAccessible(true);
      parser = ((Supplier<CalciteParser>) calciteSupplierField.get(tEnv.getParser())).get();
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    var sqlNodeList = parser.parseSqlList(sqlStatement);
    List<SqlNode> parsed = sqlNodeList.getList();
    Preconditions.checkArgument(parsed.size() == 1);
    return parsed.get(0);
  }

  public SqlNode toSqlNode(RelNode relNode) {
    return RelToFlinkSql.convertToSqlNode(relNode);
  }

  public String toSqlString(SqlNode sqlNode) {
    return RelToFlinkSql.convertToString(sqlNode);
  }

  /**
   * Builds the statement set and compiles the plan for Flink which is the final component needed
   * for the {@link FlinkPhysicalPlan}.
   *
   * @return
   */
  public FlinkPhysicalPlan compileFlinkPlan() {
    var execute = planBuilder.getExecuteStatement();
    // StatementSetOperation statmentSetOp = (StatementSetOperation) getOperation(execute);
    var insert = toSqlString(execute);
    planBuilder.add(execute, insert);
    Optional<CompiledPlan> compiledPlan = Optional.empty();
    if (executionMode == RuntimeExecutionMode.STREAMING && compileFlinkPlan) {
      var parse = (StatementSetOperation) tEnv.getParser().parse(insert + ";").get(0);
      compiledPlan = Optional.of(tEnv.compilePlan(parse.getOperations()));
    }
    return planBuilder.build(compiledPlan);
  }

  /**
   * Analyzes a view definition with the {@link SQRLLogicalPlanAnalyzer} to produce a {@link
   * ViewAnalysis}. There is some additional complexity around extracting the query from the view
   * definition and removing the top level sort (if present) since we don't want to execute that in
   * Flink but instead pull it up to the database to execute at query time.
   *
   * @param viewDef
   * @param removeTopLevelSort
   * @param hints
   * @param errors
   * @return
   */
  public ViewAnalysis analyzeView(
      SqlNode viewDef, boolean removeTopLevelSort, PlannerHints hints, ErrorCollector errors) {
    var flinkPlanner = this.validatorSupplier.get();

    var validated = flinkPlanner.validate(viewDef);
    RowLevelModificationContextUtils.clearContext();
    final SqlNode query;
    if (validated instanceof SqlCreateView view) {
      query = view.getQuery();
    } else if (validated instanceof SqlAlterViewAs as) {
      query = as.getNewQuery();
    } else {
      throw new UnsupportedOperationException("Unexpected SQLnode: " + validated);
    }
    var relRoot = toRelRoot(query, flinkPlanner);
    var relBuilder = getRelBuilder(flinkPlanner);
    var relNode = relRoot.rel;
    Optional<Sort> topLevelSort = Optional.empty();
    if (removeTopLevelSort) {
      if (relNode instanceof Sort sort) {
        // Remove top-level sort and attach it to TableAnalysis later
        topLevelSort = Optional.of(sort);
        relNode = topLevelSort.get().getInput();
      } else {
        errors.warn("Expected top-level sort on relnode: %s", relNode.explain());
      }
    }
    var analyzer =
        new SQRLLogicalPlanAnalyzer(
            relNode,
            tableLookup,
            flinkPlanner
                .getOrCreateSqlValidator()
                .getCatalogReader()
                .unwrap(CalciteCatalogReader.class),
            relBuilder,
            errors);
    var viewAnalysis = analyzer.analyze(hints);
    viewAnalysis.getTableAnalysis().topLevelSort(topLevelSort);
    return viewAnalysis;
  }

  public RelRoot toRelRoot(SqlNode query, @Nullable FlinkPlannerImpl flinkPlanner) {
    if (flinkPlanner == null) {
      flinkPlanner = this.validatorSupplier.get();
    }
    var context = new SqlNodeConvertContext(flinkPlanner, catalogManager);
    var validatedQuery = context.getSqlValidator().validate(query);
    return context.toRelRoot(validatedQuery);
  }

  public FlinkRelBuilder getRelBuilder(@Nullable FlinkPlannerImpl flinkPlanner) {
    if (flinkPlanner == null) {
      flinkPlanner = this.validatorSupplier.get();
    }
    var config =
        flinkPlanner.config().getSqlToRelConverterConfig().withAddJsonTypeOperatorEnabled(false);
    // We are using a null schema because using the scan method on FlinkRelBuilder tries to expand
    // views.
    // Need to construct LogicalTableScan manually.
    return (FlinkRelBuilder)
        config
            .getRelBuilderFactory()
            .create(flinkPlanner.cluster(), null)
            .transform(config.getRelBuilderConfigTransform());
  }

  public Function<Expression, ResolvedExpression> getExpressionResolver() {
    var flinkPlanner = this.validatorSupplier.get();
    final var relBuilder = getRelBuilder(flinkPlanner);
    final var context = unwrapContext(relBuilder);
    final var parser = tEnv.getParser();
    final var expressionResolver =
        ExpressionResolver.resolverFor(
                context.getTableConfig(),
                context.getClassLoader(),
                name -> Optional.empty(),
                context.getFunctionCatalog().asLookup(parser::parseIdentifier),
                context.getCatalogManager().getDataTypeFactory(),
                parser::parseSqlExpression)
            .build();
    return exp -> {
      return expressionResolver.resolve(Collections.singletonList(exp)).get(0);
    };
  }

  private CalciteCatalogReader getCalciteCatalog(@Nullable FlinkPlannerImpl flinkPlanner) {
    return flinkPlanner
        .getOrCreateSqlValidator()
        .getCatalogReader()
        .unwrap(CalciteCatalogReader.class);
  }

  public FlinkRelBuilder getTableScan(ObjectIdentifier identifier) {
    var flinkPlanner = this.validatorSupplier.get();
    var relBuilder = getRelBuilder(flinkPlanner);
    var catalog = getCalciteCatalog(flinkPlanner);
    relBuilder.push(
        LogicalTableScan.create(
            flinkPlanner.cluster(), catalog.getTableForMember(identifier.toList()), List.of()));
    return relBuilder;
  }

  public SqlNode getQueryFromView(SqlNode viewDef) {
    return viewDef instanceof SqlCreateView scv
        ? scv.getQuery()
        : ((SqlAlterViewAs) viewDef).getNewQuery();
  }

  /**
   * Creates a new view with the updated query
   *
   * @param updatedQuery
   * @param viewDef
   * @return
   */
  public SqlNode updateViewQuery(SqlNode updatedQuery, SqlNode viewDef) {
    if (viewDef instanceof SqlCreateView createView) {
      return updatedQuery == createView.getQuery()
          ? createView
          : new SqlCreateView(
              createView.getParserPosition(),
              createView.getViewName(),
              createView.getFieldList(),
              updatedQuery,
              createView.getReplace(),
              createView.isTemporary(),
              createView.isIfNotExists(),
              createView.getComment().orElse(null),
              createView.getProperties().orElse(null));
    } else {
      var alterView = (SqlAlterViewAs) viewDef;
      return updatedQuery == alterView.getNewQuery()
          ? alterView
          : new SqlAlterViewAs(
              alterView.getParserPosition(), alterView.getViewIdentifier(), updatedQuery);
    }
  }

  /**
   * Adds a view to Flink and produces the {@link TableAnalysis} for the planner and the DAG.
   *
   * @param originalSql
   * @param hints
   * @param errors
   * @return
   */
  public TableAnalysis addView(String originalSql, PlannerHints hints, ErrorCollector errors) {
    var viewDef = parseSQL(originalSql);
    Preconditions.checkArgument(
        viewDef instanceof SqlCreateView || viewDef instanceof SqlAlterViewAs,
        "Unexpected view definition: " + viewDef);
    /* Stage 1: Query rewriting
     In this stage, we try to pull up/out any operators that we want to rewrite as we plan the DAG.
     We attach those to the TableAnalysis so they can be re-attached during DAG planning.
     Note, that the actual "pulling out" happens during RelNode analysis
     in stage 2. In stage 1, we just finalize the SqlNode that gets passed to Flink.
     Step 1.1: If query has a top level order, we pull it out, so we can later add it to the query if necessary.
    */
    final var originalQuery = getQueryFromView(viewDef);
    final var query = removeSort(originalQuery);
    var removedSort = originalQuery != query;
    final var rewrittenViewDef = updateViewQuery(query, viewDef);
    // Add the view to Flink using the rewritten SqlNode from stage 1.
    var op = executeSqlNode(rewrittenViewDef);
    ObjectIdentifier identifier;
    Schema schema;
    if (op instanceof AlterViewAsOperation operation) {
      identifier = operation.getViewIdentifier();
      schema = operation.getNewView().getUnresolvedSchema();
      tableLookup.removeView(identifier); // remove previously planned view
    } else if (op instanceof CreateViewOperation operation) {
      identifier = operation.getViewIdentifier();
      schema = operation.getCatalogView().getUnresolvedSchema();
    } else {
      throw new UnsupportedOperationException(op.getClass().toString());
    }

    /* Stage 2: Analyze the RelNode/RelRoot
       - pull out top-level sort
     NOTE: Flink modifies the SqlSelect node during validation, so we have to re-create it from the original SQL
    */
    var viewDef2 = parseSQL(originalSql);
    var viewAnalysis = analyzeView(viewDef2, removedSort, hints, errors);
    var tableAnalysis =
        viewAnalysis
            .getTableAnalysis()
            .objectIdentifier(identifier)
            .originalSql(originalSql)
            .build();
    tableLookup.registerTable(tableAnalysis);

    return tableAnalysis;
  }

  private SqlNode removeSort(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOrderBy by) {
      return by.query;
    }
    return sqlNode;
  }

  /**
   * Parses a {@link SqrlTableFunction} definition and analyzes the result. It invokes {@link
   * #analyzeView(SqlNode, boolean, PlannerHints, ErrorCollector)} and in addition contains the
   * logic for resolving the function arguments and their types.
   *
   * @param identifier
   * @param originalSql
   * @param arguments
   * @param argumentIndexMap
   * @param hints
   * @param errors
   * @return
   */
  public SqrlTableFunction.SqrlTableFunctionBuilder resolveSqrlTableFunction(
      ObjectIdentifier identifier,
      String originalSql,
      List<ParsedArgument> arguments,
      Map<Integer, Integer> argumentIndexMap,
      PlannerHints hints,
      ErrorCollector errors) {
    List<FunctionParameter> parameters = new ArrayList<>();
    for (var i = 0; i < arguments.size(); i++) {
      var parsedArg = arguments.get(i);
      var type = parsedArg.getResolvedRelDataType();
      parameters.add(
          new SqrlFunctionParameter(
              parsedArg.getName().get(),
              parsedArg.getIndex(),
              type,
              parsedArg.isParentField(),
              parsedArg.getResolvedMetadata()));
    }
    // Analyze Query
    var funcDef2 = parseSQL(originalSql);
    var viewAnalysis = analyzeView(funcDef2, false, hints, errors);
    // Remap parameters in query so the RexDynamicParam point directly at the function parameter by
    // index
    var updateParameters =
        viewAnalysis.getRelNode().accept(new DynamicParameterReplacer(argumentIndexMap));
    var tblBuilder = viewAnalysis.getTableAnalysis();
    tblBuilder.collapsedRelnode(updateParameters);
    tblBuilder.objectIdentifier(identifier);
    tblBuilder.originalSql(originalSql);
    var tableAnalysis = tblBuilder.build();
    // Build table function
    var fctBuilder =
        SqrlTableFunction.builder()
            .functionAnalysis(tableAnalysis)
            .parameters(parameters)
            .multiplicity(SqrlTableFunction.getMultiplicity(updateParameters));
    return fctBuilder;
  }

  /**
   * Adds {@link SqrlTableFunction} for internally defined table access functions in the {@link
   * SqlScriptPlanner}.
   *
   * @param identifier
   * @param relNode
   * @param parameters
   * @param baseTable
   * @return
   */
  public SqrlTableFunction.SqrlTableFunctionBuilder addSqrlTableFunction(
      ObjectIdentifier identifier,
      RelNode relNode,
      List<FunctionParameter> parameters,
      TableAnalysis baseTable) {
    var sql = toSqlString(toSqlNode(relNode));
    var tableAnalysis =
        TableAnalysis.builder()
            .originalRelnode(relNode)
            .originalSql(sql)
            .type(baseTable.getType())
            .primaryKey(baseTable.getPrimaryKey())
            .optionalBaseTable(Optional.of(baseTable.getBaseTable()))
            .streamRoot(baseTable.getStreamRoot())
            .fromTables(List.of(baseTable))
            .hints(baseTable.getHints())
            .errors(baseTable.getErrors())
            .collapsedRelnode(relNode)
            .objectIdentifier(identifier)
            .build();
    var fctBuilder =
        SqrlTableFunction.builder()
            .functionAnalysis(tableAnalysis)
            .parameters(parameters)
            .multiplicity(SqrlTableFunction.getMultiplicity(relNode));
    return fctBuilder;
  }

  /**
   * Replaces Dynamic Parameters to use their argument index from the function signature. Apache
   * Calcite does not support dynamic parameter indexes in the parser, so all parameters are `?`. We
   * iterate through them and map them back to the index of the parameter from the signature.
   */
  @AllArgsConstructor
  private static class DynamicParameterReplacer extends RelShuttleImpl {

    final Map<Integer, Integer> argumentIndexMap;
    final RexShuttle rexShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            int newIndex = argumentIndexMap.get(dynamicParam.getIndex());
            if (newIndex != dynamicParam.getIndex()) {
              return new RexDynamicParam(dynamicParam.getType(), newIndex);
            } else {
              return dynamicParam;
            }
          }
        };

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof LogicalTableFunctionScan scan) {
        return visit(scan);
      }
      return super.visit(other);
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
      var call = (RexCall) scan.getCall().accept(rexShuttle);
      return scan.copy(
          scan.getTraitSet(),
          scan.getInputs(),
          call,
          scan.getElementType(),
          scan.getRowType(),
          scan.getColumnMappings());
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      if (i == 0) {
        parent = parent.accept(rexShuttle);
      }
      return super.visitChild(parent, i, child);
    }
  }

  public void registerSqrlTableFunction(SqrlTableFunction function) {
    sqrlFunctionCatalog.addFunction(function);
  }

  @FunctionalInterface
  public interface MutationBuilder {
    MutationQueryBuilder createMutation(
        FlinkTableBuilder flinkTableBuilder, RelDataType relDataType);
  }

  public TableAnalysis createTableWithSchema(
      String tableName,
      String tableDefinition,
      Optional<RelDataType> schema,
      MutationBuilder logEngineBuilder) {
    return addSourceTable(
        addTable(Optional.of(tableName), tableDefinition, schema, logEngineBuilder));
  }

  public ObjectIdentifier addExternalExport(
      String tableName, String tableDefinition, Optional<RelDataType> schema) {
    var result =
        addTable(
            Optional.of(tableName),
            tableDefinition,
            schema,
            (x, y) -> {
              throw new UnsupportedOperationException(
                  "Export tables require connector configuration");
            });
    return result.baseTableIdentifier;
  }

  public Optional<TableAnalysis> createTable(
      String tableDefinition, MutationBuilder logEngineBuilder) {
    var result = addTable(Optional.empty(), tableDefinition, Optional.empty(), logEngineBuilder);
    if (result.isSourceTable()) return Optional.of(addSourceTable(result));
    else return Optional.empty();
  }

  public SqlCreateView createScanView(String viewName, ObjectIdentifier id) {
    return FlinkSqlNodeFactory.createView(
        viewName, FlinkSqlNodeFactory.selectAllFromTable(FlinkSqlNodeFactory.identifier(id)));
  }

  private static final String TEMP_VIEW_SUFFIX = "__view";

  /**
   * We add a view on top of the created table with the name of the table. The reason we "cover"
   * CREATE TABLE statements with a view is because Flink expands references to physical tables by
   * adding computed columns and watermark, thus making it very difficult to reconcile the DAG
   * because of that repetition. By adding a view on top, we get a stable reference to the expanded
   * table that we can add to the tableLookup for resolution.
   *
   * @param addResult
   * @return
   */
  private TableAnalysis addSourceTable(AddTableResult addResult) {
    var view =
        createScanView(addResult.tableName + TEMP_VIEW_SUFFIX, addResult.baseTableIdentifier);
    var viewAnalysis = analyzeView(view, false, PlannerHints.EMPTY, ErrorCollector.root());
    TableAnalysis.TableAnalysisBuilder tbBuilder = viewAnalysis.getTableAnalysis();
    tbBuilder.objectIdentifier(addResult.baseTableIdentifier).originalSql(toSqlString(view));
    // Remove trivial LogicalProject so that subsequent references match
    RelNode relNode = tbBuilder.build().getOriginalRelnode();
    if (CalciteUtil.isTrivialProject(relNode)) relNode = relNode.getInput(0);
    var tableAnalysis = tbBuilder.originalRelnode(relNode).collapsedRelnode(relNode).build();
    tableLookup.registerTable(tableAnalysis);
    return tableAnalysis;
  }

  @Value
  private static class AddTableResult {
    String tableName;
    ObjectIdentifier baseTableIdentifier;
    boolean isSourceTable;
  }

  /**
   * Adds a table to Flink and analyzes the table for schema and primary key definition. If the
   * table does not have a connector, it is a mutation and we generate the connector via the
   * provided mutationBuilder.
   *
   * @param tableName
   * @param createTableSql
   * @param schema
   * @param mutationBuilder
   * @return
   */
  private AddTableResult addTable(
      Optional<String> tableName,
      String createTableSql,
      Optional<RelDataType> schema,
      MutationBuilder mutationBuilder) {
    var tableSqlNode = parseSQL(createTableSql);
    Preconditions.checkArgument(
        tableSqlNode instanceof SqlCreateTable, "Expected CREATE TABLE statement");
    var tableDefinition = (SqlCreateTable) tableSqlNode;
    var fullTable = tableDefinition;
    final var finalTableName = tableName.orElse(tableDefinition.getTableName().getSimple());
    if (schema.isPresent()) {
      // Use LIKE to merge schema with table definition
      var schemaTableName = finalTableName + SCHEMA_SUFFIX;
      // This should be a temporary table
      var schemaTable = FlinkSqlNodeFactory.createTable(schemaTableName, schema.get(), true);
      executeSqlNode(schemaTable);

      var likeClause =
          new SqlTableLike(
              SqlParserPos.ZERO, FlinkSqlNodeFactory.identifier(schemaTableName), List.of());
      fullTable =
          new SqlCreateTableLike(
              tableDefinition.getParserPosition(),
              FlinkSqlNodeFactory.identifier(finalTableName),
              tableDefinition.getColumnList(),
              tableDefinition.getTableConstraints(),
              tableDefinition.getPropertyList(),
              //          tableDefinition.getDistribution(),
              tableDefinition.getPartitionKeyList(),
              tableDefinition.getWatermark().orElse(null),
              tableDefinition.getComment().orElse(null),
              likeClause,
              tableDefinition.isTemporary(),
              tableDefinition.ifNotExists);
    } else {
      // Replace name but leave everything else
      fullTable =
          new SqlCreateTable(
              tableDefinition.getParserPosition(),
              FlinkSqlNodeFactory.identifier(finalTableName),
              tableDefinition.getColumnList(),
              tableDefinition.getTableConstraints(),
              tableDefinition.getPropertyList(),
              //          tableDefinition.getDistribution(),
              tableDefinition.getPartitionKeyList(),
              tableDefinition.getWatermark().orElse(null),
              tableDefinition.getComment().orElse(null),
              tableDefinition.isTemporary(),
              tableDefinition.ifNotExists);
    }
    MutationQueryBuilder mutationBld = null;
    if (fullTable.getPropertyList().isEmpty()) { // it's an internal CREATE TABLE for a mutation
      var tableBuilder = FlinkTableBuilder.toBuilder(fullTable);
      tableBuilder.setName(finalTableName);
      /* TODO: We want to create the table with a datagen connector so we can fully plan it
      and get the relnode for a tablescan. That allows us to pull out any computed columns (and the RexCalls)
      from the projection. This will also give us the relDataType which we currently set to null as
      we make some strong simplifying assumptions here.
      Note, that this requires we replace the table with the actual table (and the correct connector)
      in the schema with an ALTER TABLE statement.
       */
      mutationBld = mutationBuilder.createMutation(tableBuilder, null);
      fullTable = tableBuilder.buildSql(false);
    }
    var tableOp = (CreateTableOperation) executeSqlNode(fullTable);
    // Create table analysis
    var flinkSchema = ((ResolvedCatalogTable) tableOp.getCatalogTable()).getResolvedSchema();
    // Map primary key
    var pk =
        flinkSchema
            .getPrimaryKey()
            .map(
                flinkPk ->
                    PrimaryKeyMap.of(
                        flinkPk.getColumns().stream()
                            .map(
                                name ->
                                    IntStream.range(0, flinkSchema.getColumns().size())
                                        .filter(
                                            i ->
                                                flinkSchema
                                                    .getColumns()
                                                    .get(i)
                                                    .getName()
                                                    .equalsIgnoreCase(name))
                                        .findFirst()
                                        .getAsInt())
                            .collect(Collectors.toList())))
            .orElse(PrimaryKeyMap.UNDEFINED);
    // Finish building mutation query by building input and output types from table schema
    if (mutationBld != null) {
      var fields = convertSchema2RelDataType(flinkSchema);
      var inputType = CalciteUtil.getRelTypeBuilder(typeFactory);
      var outputType = CalciteUtil.getRelTypeBuilder(typeFactory);
      var computedColumns = mutationBld.build().getComputedColumns();
      for (var i = 0; i < flinkSchema.getColumns().size(); i++) {
        var field = fields.get(i);
        var column = flinkSchema.getColumns().get(i);
        outputType.add(field);
        // Check if field is a computed column, if so it should not be part of input type
        var computedColumn =
            computedColumns.stream()
                .filter(col -> col.getColumnName().equals(column.getName()))
                .findFirst();
        if (computedColumn.isPresent()) {
          // if computed column is UUID and we don't have a pk, select it as pk
          if (pk.isUndefined()
              && computedColumn.get().getType() == MutationComputedColumnType.UUID) {
            pk = PrimaryKeyMap.of(List.of(i));
          }
        } else {
          inputType.add(field);
        }
      }
      mutationBld.inputDataType(inputType.build());
      mutationBld.outputDataType(outputType.build());
    }
    ObjectIdentifier tableId = tableOp.getTableIdentifier();
    var connector = new FlinkConnectorConfig(tableOp.getCatalogTable().getOptions());
    var tableAnalysis =
        TableAnalysis.of(
            tableId,
            new SourceSinkTableAnalysis(
                connector, flinkSchema, mutationBld != null ? mutationBld.build() : null),
            connector.getTableType(),
            pk);
    tableLookup.registerTable(tableAnalysis);

    return new AddTableResult(finalTableName, tableId, connector.isSourceConnector());
  }

  public ObjectIdentifier createSinkTable(FlinkTableBuilder tableBuilder) {
    return ((CreateTableOperation) executeSqlNode(tableBuilder.buildSql(false)))
        .getTableIdentifier();
  }

  public void insertInto(RelNode relNode, ObjectIdentifier sinkTableId) {
    var selectQuery = toSqlNode(relNode);
    planBuilder.addInsert(FlinkSqlNodeFactory.createInsert(selectQuery, sinkTableId));
  }

  public void insertInto(RichSqlInsert insert) {
    planBuilder.addInsert(insert);
  }

  public SqlOperator lookupUserDefinedFunction(FunctionDefinition fct) {
    var name = getFunctionNameFromClass(fct.getClass()).getDisplay();
    List<SqlOperator> list = new ArrayList<>();
    var flinkPlanner = this.validatorSupplier.get();
    flinkPlanner
        .getOrCreateSqlValidator()
        .getOperatorTable()
        .lookupOperatorOverloads(
            new SqlIdentifier(name, SqlParserPos.ZERO),
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            SqlSyntax.FUNCTION,
            list,
            SqlNameMatchers.liberal());
    Preconditions.checkArgument(!list.isEmpty(), "Could not find function: " + name);
    return list.get(0);
  }

  public void addUserDefinedFunction(String name, String clazz, boolean isSystem) {
    var functionSql = FlinkSqlNodeFactory.createFunction(name, clazz, isSystem);
    var addFctOp = executeSqlNode(functionSql);
    // Function definitions are not in the compiled plan, have to add them explicitly but with fully
    // resolved identifier
    if (addFctOp instanceof CreateCatalogFunctionOperation operation) {
      functionSql =
          FlinkSqlNodeFactory.createFunction(
              FlinkSqlNodeFactory.identifier(operation.getFunctionIdentifier()), clazz, isSystem);
    }
    planBuilder.addFullyResolvedFunction(toSqlString(functionSql));
  }

  private static void checkResultOk(TableResultInternal result) {
    Preconditions.checkArgument(
        result == TableResultInternal.TABLE_RESULT_OK, "Result is not OK: %s", result);
  }

  private List<RelDataTypeField> convertSchema2RelDataType(ResolvedSchema schema) {
    return parseSchema(schema).stream().map(ParsedRelDataTypeResult::field).toList();
  }

  private List<ParsedRelDataTypeResult> parseSchema(ResolvedSchema schema) {
    List<ParsedRelDataTypeResult> fields = new ArrayList<>();
    Function<Expression, ResolvedExpression> expressionResolver = null;
    for (var i = 0; i < schema.getColumns().size(); i++) {
      var column = schema.getColumns().get(i);
      AbstractDataType type = null;
      Optional<String> metadata = Optional.empty();
      Optional<RexNode> expression = Optional.empty();
      if (column instanceof PhysicalColumn physicalColumn) {
        type = physicalColumn.getDataType();
      } else if (column instanceof MetadataColumn metadataColumn) {
        type = metadataColumn.getDataType();
        metadata = metadataColumn.getMetadataKey();
      } else if (column instanceof ComputedColumn computedColumn) {
        type = computedColumn.getDataType();
        expression = Optional.of(((RexNodeExpression) computedColumn.getExpression()).getRexNode());
      }
      if (type instanceof DataType dataType) {
        fields.add(
            new ParsedRelDataTypeResult(
                new RelDataTypeFieldImpl(
                    column.getName(),
                    i,
                    typeFactory.createFieldTypeFromLogicalType(dataType.getLogicalType())),
                metadata,
                expression));
      } else {
        throw new StatementParserException(
            ErrorLabel.GENERIC, new FileLocation(i, 1), "Invalid type: " + column);
      }
    }
    return fields;
  }

  public record ParsedRelDataTypeResult(
      RelDataTypeField field, Optional<String> metadata, Optional<RexNode> expression) {}

  private static final String DATATYPE_PARSING_PREFIX =
      "CREATE TEMPORARY TABLE __sqrlinternal_types( ";

  /**
   * Uses a CREATE TABLE statement to parse the data types from a string
   *
   * @param dataTypeDefinition
   * @return
   */
  public List<ParsedRelDataTypeResult> parse2RelDataType(ParsedObject<String> dataTypeDefinition) {
    if (dataTypeDefinition.isEmpty()) {
      return List.of();
    }
    var createTableStatement =
        DATATYPE_PARSING_PREFIX + dataTypeDefinition.get() + " ) WITH ('connector' = 'filesystem')";
    try {
      var op = (CreateTableOperation) getOperation(parseSQL(createTableStatement));
      var schema = ((ResolvedCatalogTable) op.getCatalogTable()).getResolvedSchema();
      return parseSchema(schema);
    } catch (Exception e) {
      var location = dataTypeDefinition.getFileLocation();
      var converted = ParsePosUtil.convertFlinkParserException(e);
      if (converted.isPresent()) {
        location =
            location.add(
                SQLStatement.removeFirstRowOffset(
                    converted.get().getLocation(), DATATYPE_PARSING_PREFIX.length()));
      }
      throw new StatementParserException(
          location, e, converted.map(MessageLocation::getMessage).orElse(e.getMessage()));
    }
  }

  @SneakyThrows
  public Operation executeSQL(String sqlStatement) {
    return executeSqlNode(parseSQL(sqlStatement));
  }

  public Operation executeSqlNode(SqlNode sqlNode) {
    planBuilder.add(sqlNode, this);
    var operation = getOperation(sqlNode);
    checkResultOk(tEnv.executeInternal(operation));
    return operation;
  }

  private Operation getOperation(SqlNode sqlNode) {
    return SqlNodeToOperationConversion.convert(validatorSupplier.get(), catalogManager, sqlNode)
        .orElseThrow(() -> new TableException("Unsupported query: " + sqlNode));
  }

  private static List<URL> getUdfUrls(BuildPath buildPath) {
    List<URL> urls = new ArrayList<>();
    try (var stream = Files.newDirectoryStream(buildPath.getUdfPath(), "*.jar")) {
      stream.forEach(
          p -> {
            try {
              urls.add(p.toUri().toURL());
            } catch (MalformedURLException e) {
              throw new RuntimeException(e);
            }
          });
    } catch (IOException e) {
      // Means there is no lib directory
    }
    return urls;
  }
}
