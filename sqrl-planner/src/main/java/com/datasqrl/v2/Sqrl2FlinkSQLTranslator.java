package com.datasqrl.v2;

import static com.datasqrl.function.FlinkUdfNsObject.getFunctionNameFromClass;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.config.BuildPath;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import com.datasqrl.v2.FlinkPhysicalPlan.Builder;
import com.datasqrl.v2.analyzer.SQRLLogicalPlanAnalyzer;
import com.datasqrl.v2.analyzer.SQRLLogicalPlanAnalyzer.ViewAnalysis;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.dag.plan.MutationQuery.MutationQueryBuilder;
import com.datasqrl.v2.hint.PlannerHints;
import com.datasqrl.v2.parser.ParsedField;
import com.datasqrl.v2.parser.SqrlTableFunctionStatement.ParsedArgument;
import com.datasqrl.v2.parser.StatementParserException;
import com.datasqrl.v2.tables.FlinkConnectorConfig;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import com.datasqrl.v2.tables.MutationComputedColumn;
import com.datasqrl.v2.tables.MutationComputedColumn.Type;
import com.datasqrl.v2.tables.SourceSinkTableAnalysis;
import com.datasqrl.v2.tables.SqrlFunctionParameter;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.StdLibrary;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.SqlNodeConvertContext;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

public class Sqrl2FlinkSQLTranslator {

  public static final String SCHEMA_SUFFIX = "__schema";
  public static final String TABLE_DEFINITION_SUFFIX = "__def";

  private final StreamTableEnvironmentImpl tEnv;
  private final Supplier<FlinkPlannerImpl> validatorSupplier;
  private final Supplier<CalciteParser> calciteSupplier;
  private final SqrlFunctionCatalog sqrlFunctionCatalog;
  private final CatalogManager catalogManager;
  private final FlinkTypeFactory typeFactory;

  @Getter
  private final TableAnalysisLookup tableLookup = new TableAnalysisLookup();
  private final FlinkPhysicalPlan.Builder planBuilder = new Builder();

  public Sqrl2FlinkSQLTranslator(BuildPath buildPath) {
    List<URL> jarUrls = getUdfUrls(buildPath);
    ClassLoader udfClassLoader = new URLClassLoader(jarUrls.toArray(new URL[0]), getClass().getClassLoader());
    Map<String, String> config = new HashMap<>();
    config.put("pipeline.classpaths", jarUrls.stream().map(URL::toString)
        .collect(Collectors.joining(",")));
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(
        Configuration.fromMap(config));

    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(config))
        .withClassLoader(udfClassLoader)
        .build();

    this.tEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(sEnv, tEnvConfig);
    this.validatorSupplier = ((PlannerBase)tEnv.getPlanner())::createFlinkPlanner;
    FlinkPlannerImpl planner = this.validatorSupplier.get();
    typeFactory = (FlinkTypeFactory) planner.getOrCreateSqlValidator().getTypeFactory();
    sqrlFunctionCatalog = new SqrlFunctionCatalog(typeFactory);
    CalciteConfigBuilder calciteConfigBuilder = new CalciteConfigBuilder();
    calciteConfigBuilder.addSqlOperatorTable(sqrlFunctionCatalog.getOperatorTable());
    this.tEnv.getConfig().setPlannerConfig(calciteConfigBuilder.build());
    this.catalogManager = tEnv.getCatalogManager();

    try {
      //TODO: This is a hack - is there a better way to get the calcite parser?
      Field calciteSupplierField = ParserImpl.class.getDeclaredField("calciteParserSupplier");
      calciteSupplierField.setAccessible(true);
      this.calciteSupplier = (Supplier<CalciteParser>) calciteSupplierField.get(tEnv.getParser());
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    //Register SQRL standard functions
    StreamUtil.filterByClass(ServiceLoaderDiscovery.getAll(StdLibrary.class), AbstractFunctionModule.class)
        .flatMap(fctModule -> fctModule.getFunctions().stream())
        .forEach(fct -> this.addUserDefinedFunction(fct.getSqlName(), fct.getFunction().getClass().getName(), true));

  }

  public SqrlRexUtil getRexUtil() {
    return new SqrlRexUtil(typeFactory);
  }


  private SqlNodeConvertContext getConvertContext() {
    return new SqlNodeConvertContext(validatorSupplier.get(), catalogManager);
  }

  public SqlNode parseSQL(String sqlStatement) {
    SqlNodeList sqlNodeList = calciteSupplier.get().parseSqlList(sqlStatement);
    List<SqlNode> parsed = sqlNodeList.getList();
    Preconditions.checkArgument(parsed.size() == 1);
    return parsed.get(0);
  }

  public Operation parseOperation(String sqlStatement) {
    return tEnv.getParser().parse(sqlStatement).get(0);
  }

  public SqlNode toSqlNode(RelNode relNode) {
    return RelToFlinkSql.convertToSqlNode(relNode);
  }

  public String toSqlString(SqlNode sqlNode) {
    return RelToFlinkSql.convertToString(sqlNode);
  }

  public FlinkPhysicalPlan compilePlan() {
    SqlExecute execute = planBuilder.getExecuteStatement();
    //StatementSetOperation statmentSetOp = (StatementSetOperation) getOperation(execute);
    String insert = toSqlString(execute);
    planBuilder.add(execute, insert);
    StatementSetOperation parse = (StatementSetOperation)tEnv.getParser().parse(insert + ";").get(0);
    CompiledPlan compiledPlan = tEnv.compilePlan(parse.getOperations());

//    CompiledPlan compiledPlan = tEnv.compilePlanSql(execute.toString());
//    CompiledPlan compiledPlan = tEnv.compilePlan(statmentSetOp.getOperations());
    return planBuilder.build(compiledPlan);
  }



  public ViewAnalysis analyzeView(SqlNode viewDef, boolean removeTopLevelSort,
      PlannerHints hints, ErrorCollector errors) {
    FlinkPlannerImpl flinkPlanner = this.validatorSupplier.get();

    SqlNode validated = flinkPlanner.validate(viewDef);
    RowLevelModificationContextUtils.clearContext();
    final SqlNode query;
    if (validated instanceof SqlCreateView) {
      query = ((SqlCreateView) validated).getQuery();
    } else if (validated instanceof SqlAlterViewAs) {
      query = ((SqlAlterViewAs) validated).getNewQuery();
    } else throw new UnsupportedOperationException("Unexpected SQLnode: " + validated);
    RelRoot relRoot = toRelRoot(query, flinkPlanner);
    FlinkRelBuilder relBuilder = getRelBuilder(flinkPlanner);
    RelNode relNode = relRoot.rel;
    Optional<Sort> topLevelSort = Optional.empty();
    if (removeTopLevelSort) {
      if (relNode instanceof Sort) {
        //Remove top-level sort and attach it to TableAnalysis later
        topLevelSort = Optional.of((Sort) relNode);
        relNode = topLevelSort.get().getInput();
      } else {
        errors.warn("Expected top-level sort on relnode: %s",relNode.explain());
      }
    }
    SQRLLogicalPlanAnalyzer analyzer = new SQRLLogicalPlanAnalyzer(relNode, tableLookup,
        flinkPlanner.getOrCreateSqlValidator().getCatalogReader().unwrap(CalciteCatalogReader.class),
        relBuilder,
        errors);
    ViewAnalysis viewAnalysis = analyzer.analyze(hints);
    viewAnalysis.getTableAnalysis().topLevelSort(topLevelSort);
    return viewAnalysis;
  }

  public RelRoot toRelRoot(SqlNode query, @Nullable FlinkPlannerImpl flinkPlanner) {
    if (flinkPlanner == null) flinkPlanner = this.validatorSupplier.get();
    SqlNodeConvertContext context = new SqlNodeConvertContext(flinkPlanner, catalogManager);
    SqlNode validatedQuery = context.getSqlValidator().validate(query);
    return context.toRelRoot(validatedQuery);
  }

  public FlinkRelBuilder getRelBuilder(@Nullable FlinkPlannerImpl flinkPlanner) {
    if (flinkPlanner == null) flinkPlanner = this.validatorSupplier.get();
    SqlToRelConverter.Config config = flinkPlanner.config().getSqlToRelConverterConfig().withAddJsonTypeOperatorEnabled(false);
    //We are using a null schema because using the scan method on FlinkRelBuilder tries to expand views.
    //Need to construct LogicalTableScan manually.
    return
        (FlinkRelBuilder) config.getRelBuilderFactory()
            .create(flinkPlanner.cluster(), null)
            .transform(config.getRelBuilderConfigTransform());
  }

  private CalciteCatalogReader getCalciteCatalog(@Nullable FlinkPlannerImpl flinkPlanner) {
    return flinkPlanner.getOrCreateSqlValidator().getCatalogReader().unwrap(CalciteCatalogReader.class);
  }

  public FlinkRelBuilder getTableScan(ObjectIdentifier identifier) {
    FlinkPlannerImpl flinkPlanner = this.validatorSupplier.get();
    FlinkRelBuilder relBuilder = getRelBuilder(flinkPlanner);
    CalciteCatalogReader catalog = getCalciteCatalog(flinkPlanner);
    relBuilder.push(LogicalTableScan.create(flinkPlanner.cluster(), catalog.getTableForMember(identifier.toList()), List.of()));
    return relBuilder;
  }

  private SqlNode removeSort(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOrderBy) {
      return ((SqlOrderBy)sqlNode).query;
    }
    return sqlNode;
  }

  public SqlNode getQueryFromView(SqlNode viewDef) {
    return viewDef instanceof SqlCreateView?
        ((SqlCreateView) viewDef).getQuery():
        ((SqlAlterViewAs)viewDef).getNewQuery();
  }

  public SqlNode updateViewQuery(SqlNode updatedQuery, SqlNode viewDef) {
    if (viewDef instanceof SqlCreateView) {
      SqlCreateView createView = (SqlCreateView) viewDef;
      return updatedQuery==createView.getQuery()?createView:
          new SqlCreateView(createView.getParserPosition(),
              createView.getViewName(), createView.getFieldList(), updatedQuery, createView.getReplace(),
              createView.isTemporary(), createView.isIfNotExists(),
              createView.getComment().orElse(null), createView.getProperties().orElse(null));
    } else {
      SqlAlterViewAs alterView = (SqlAlterViewAs) viewDef;
      return updatedQuery==alterView.getNewQuery()?alterView:
          new SqlAlterViewAs(alterView.getParserPosition(), alterView.getViewIdentifier(), updatedQuery);
    }
  }

  public TableAnalysis addView(String originalSql, PlannerHints hints, ErrorCollector errors) {
    SqlNode viewDef = parseSQL(originalSql);
    Preconditions.checkArgument(viewDef instanceof SqlCreateView || viewDef instanceof SqlAlterViewAs,
        "Unexpected view definition: " + viewDef);
    /* Stage 1: Query rewriting
      In this stage, we try to pull up/out any operators that we want to rewrite as we plan the DAG.
      We attach those to the TableAnalysis so they can be re-attached during DAG planning.
      Note, that the actual "pulling out" happens during RelNode analysis
      in stage 2. In stage 1, we just finalize the SqlNode that gets passed to Flink.
      Step 1.1: If query has a top level order, we pull it out, so we can later add it to the query if necessary.
     */
    final SqlNode originalQuery = getQueryFromView(viewDef);
    final SqlNode query = removeSort(originalQuery);
    boolean removedSort = originalQuery!=query;
    final SqlNode rewrittenViewDef = updateViewQuery(query, viewDef);
    // Add the view to Flink using the rewritten SqlNode from stage 1.
    Operation op = executeSqlNode(rewrittenViewDef);
    ObjectIdentifier identifier;
    Schema schema;
    if (op instanceof AlterViewAsOperation) {
      identifier = ((AlterViewAsOperation) op).getViewIdentifier();
      schema = ((AlterViewAsOperation) op).getNewView().getUnresolvedSchema();
    } else if (op instanceof CreateViewOperation) {
      identifier = ((CreateViewOperation) op).getViewIdentifier();
      schema = ((CreateViewOperation) op).getCatalogView().getUnresolvedSchema();
    }
    else throw new UnsupportedOperationException(op.getClass().toString());


    /* Stage 2: Analyze the RelNode/RelRoot
        - pull out top-level sort
      NOTE: Flink modifies the SqlSelect node during validation, so we have to re-create it from the original SQL
     */
    //
    ViewAnalysis viewAnalysis = analyzeView(parseSQL(originalSql), removedSort, hints, errors);
    TableAnalysis tableAnalysis = viewAnalysis.getTableAnalysis()
        .identifier(identifier)
        .originalSql(originalSql)
        .build();
    tableLookup.registerTable(tableAnalysis);
//    System.out.println(tableAnalysis);
//    System.out.println(identifier.getObjectName() + " PRIMARY KEY: " + (tableAnalysis.getPrimaryKey().isDefined()?tableAnalysis.getPrimaryKey().asSimpleList():""));
//    System.out.println(identifier.getObjectName() + " INPUTS: " + (tableAnalysis.getFromTables().stream().map(
//        TableOrFunctionAnalysis::getIdentifier).collect(
//        Collectors.toList())));
//    System.out.println(identifier.getObjectName() + " PLAN :" + tableAnalysis.getCollapsedRelnode().explain());

    return tableAnalysis;
  }

//  /**
//   * Produces one table function for a full scan and one for lookup by primary key
//   * @param tableAnalysis
//   * @return
//   */
//  private SqrlTableFunction makeFunction(TableAnalysis tableAnalysis) {
//    List<SqrlTableFunction> functions = new ArrayList<>();
//    //TODO: add default sort (stream?timestamp desc+ pk columns asc) if top level is not logicalsort
//    return new SqrlTableFunction(List.of(), tableAnalysis.getRowType(), tableAnalysis);
////    if (tableAnalysis.getPrimaryKey().isDefined()) {
////      RelBuilder relB = getRelBuilder(null);
////      relB.push(tableAnalysis.getRelNode());
////      List<SqrlFunctionParameter> parameters = CalciteUtil.addFilterByColumn(relB, tableAnalysis.getSimplePrimaryKey().asSimpleList());
////      functions.add(new SqrlTableFunction((List)parameters, tableAnalysis.getRowType(), tableAnalysis));
////    }
////    return functions;
//  }

  public SqrlTableFunction.SqrlTableFunctionBuilder resolveSqrlTableFunction(ObjectIdentifier identifier,
      String originalSql, List<ParsedArgument> arguments,
      Map<Integer, Integer> argumentIndexMap, PlannerHints hints, ErrorCollector errors) {
    //Process argument types
    List<ParsedField> requiresTypeParsing = arguments.stream()
        .filter(Predicate.not(ParsedArgument::hasResolvedType)).collect(Collectors.toList());
    List<RelDataTypeField> parsedTypes = parse2RelDataType(requiresTypeParsing);
    List<FunctionParameter> parameters=new ArrayList<>();
    for (int i = 0; i < arguments.size(); i++) {
      ParsedArgument parsedArg = arguments.get(i);
      RelDataType type = (i<parsedTypes.size()?parsedTypes.get(i).getType():parsedArg.getResolvedRelDataType());
      parameters.add(new SqrlFunctionParameter(parsedArg.getName().get(),
          parsedArg.getOrdinal(), type, parsedArg.isParentField()));
    }
    //Analyze Query
    SqlNode sqlNode = parseSQL(originalSql);
    ViewAnalysis viewAnalysis = analyzeView(sqlNode, false, hints, errors);
    //Remap parameters in query so the RexDynamicParam point directly at the function parameter by index
    RelNode updateParameters = viewAnalysis.getRelNode().accept(new RexShuttle() {
      @Override
      public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        int newIndex = argumentIndexMap.get(dynamicParam.getIndex());
        return new RexDynamicParam(dynamicParam.getType(), newIndex);
      }
    });
    TableAnalysis.TableAnalysisBuilder tblBuilder = viewAnalysis.getTableAnalysis();
    tblBuilder.collapsedRelnode(updateParameters);
    tblBuilder.identifier(identifier);
    tblBuilder.originalSql(originalSql);
    TableAnalysis tableAnalysis = tblBuilder.build();
    //The base table is the right-most table in the relational tree that has the same type as the result
    Optional<TableAnalysis> baseTable = Optional.ofNullable(Iterables.getLast(tableAnalysis.getFromTables(),null))
        .filter(tbl -> tbl.getRowType().equals(tableAnalysis.getRowType()))
        .filter(TableAnalysis.class::isInstance).map(TableAnalysis.class::cast);
    SqrlTableFunction.SqrlTableFunctionBuilder fctBuilder = SqrlTableFunction.builder()
        .functionAnalysis(tableAnalysis)
        .baseTable(baseTable)
        .parameters(parameters)
        .multiplicity(SqrlTableFunction.getMultiplicity(updateParameters));
    return fctBuilder;
  }

  public void registerSqrlTableFunction(SqrlTableFunction function) {
    sqrlFunctionCatalog.addFunction(function);
  }

  @FunctionalInterface
  public interface MutationBuilder {
    MutationQueryBuilder createMutation(FlinkTableBuilder flinkTableBuilder, RelDataType relDataType);
  }

  public TableAnalysis addImport(String tableName, String tableDefinition,
      Optional<RelDataType> schema,
      MutationBuilder logEngineBuilder) {
    return addSourceTable(addTable(Optional.of(tableName), tableDefinition, schema, logEngineBuilder));
  }

  public ObjectIdentifier addExternalExport(String tableName, String tableDefinition, Optional<RelDataType> schema) {
    AddTableResult result = addTable(Optional.of(tableName), tableDefinition, schema, (x,y) -> {
      throw new UnsupportedOperationException("Export tables require connector configuration");
    });
    return result.baseTableIdentifier;
  }

  public TableAnalysis createTable(String tableDefinition,
      MutationBuilder logEngineBuilder) {
    AddTableResult result = addTable(Optional.empty(), tableDefinition, Optional.empty(), logEngineBuilder);
    return addSourceTable(result);
  }

  public SqlCreateView createScanView(String viewName, ObjectIdentifier id) {
    return  FlinkSqlNodeFactory.createView(viewName, FlinkSqlNodeFactory.selectAllFromTable(
        FlinkSqlNodeFactory.identifier(id)));
  }

  private TableAnalysis addSourceTable(AddTableResult addResult) {
    SqlCreateView view = createScanView(addResult.tableName, addResult.baseTableIdentifier);
    return addView(toSqlString(view), PlannerHints.EMPTY, ErrorCollector.root());
  }

  @Value
  private static class AddTableResult {
    String tableName;
    ObjectIdentifier baseTableIdentifier;
  }

  private AddTableResult addTable(Optional<String> tableName, String createTableSql,
      Optional<RelDataType> schema, MutationBuilder mutationBuilder) {
    SqlNode tableSqlNode = parseSQL(createTableSql);
    Preconditions.checkArgument(tableSqlNode instanceof SqlCreateTable, "Expected CREATE TABLE statement");
    SqlCreateTable tableDefinition = (SqlCreateTable) tableSqlNode;
    SqlCreateTable fullTable = tableDefinition;
    final String finalTableName = tableName.orElse(tableDefinition.getTableName().getSimple());
    final String baseTableName = finalTableName+TABLE_DEFINITION_SUFFIX;
    if (schema.isPresent()) {
      //Use LIKE to merge schema with table definition
      String schemaTableName = finalTableName+SCHEMA_SUFFIX;
      //This should be a temporary table
      SqlCreateTable schemaTable = FlinkSqlNodeFactory.createTable(schemaTableName, schema.get(), true);
      executeSqlNode(schemaTable);

      SqlTableLike likeClause = new SqlTableLike(SqlParserPos.ZERO,
          FlinkSqlNodeFactory.identifier(schemaTableName),
          List.of());
      fullTable = new SqlCreateTableLike(tableDefinition.getParserPosition(),
          FlinkSqlNodeFactory.identifier(baseTableName),
          tableDefinition.getColumnList(),
          tableDefinition.getTableConstraints(),
          tableDefinition.getPropertyList(),
          tableDefinition.getPartitionKeyList(),
          tableDefinition.getWatermark().orElse(null),
          tableDefinition.getComment().orElse(null),
          likeClause,
          tableDefinition.isTemporary(),
          tableDefinition.ifNotExists);
    } else if (tableName.isPresent()) {
      //Replace name but leave everything else
      fullTable = new SqlCreateTable(tableDefinition.getParserPosition(),
          FlinkSqlNodeFactory.identifier(baseTableName),
          tableDefinition.getColumnList(),
          tableDefinition.getTableConstraints(),
          tableDefinition.getPropertyList(),
          tableDefinition.getPartitionKeyList(),
          tableDefinition.getWatermark().orElse(null),
          tableDefinition.getComment().orElse(null),
          tableDefinition.isTemporary(),
          tableDefinition.ifNotExists);
    }
    MutationQueryBuilder mutationBld = null;
    if (fullTable.getPropertyList().isEmpty()) { //it's an internal CREATE TABLE for a mutation
      FlinkTableBuilder tableBuilder = FlinkTableBuilder.toBuilder(fullTable);
      tableBuilder.setName(baseTableName);
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
    CreateTableOperation tableOp = (CreateTableOperation)executeSqlNode(fullTable);
    //Create table analysis
    Schema flinkSchema = tableOp.getCatalogTable().getUnresolvedSchema();
    //Map primary key
    PrimaryKeyMap pk = flinkSchema.getPrimaryKey()
        .map(flinkPk -> PrimaryKeyMap.of(
            flinkPk.getColumnNames().stream().map(name ->
                IntStream.range(0,flinkSchema.getColumns().size())
                    .filter(i -> flinkSchema.getColumns().get(i).getName().equalsIgnoreCase(name)).findFirst().getAsInt()
            ).collect(Collectors.toList())
        ))
        .orElse(PrimaryKeyMap.UNDEFINED);
    //Finish mutation query
    if (mutationBld!=null) {
      List<RelDataTypeField> fields = convertSchema2RelDataType(flinkSchema);
      RelDataTypeBuilder inputType = CalciteUtil.getRelTypeBuilder(typeFactory);
      RelDataTypeBuilder outputType = CalciteUtil.getRelTypeBuilder(typeFactory);
      for (int i=0; i<flinkSchema.getColumns().size(); i++) {
        RelDataTypeField field = fields.get(i);
        UnresolvedColumn column = flinkSchema.getColumns().get(i);
        outputType.add(field);
        if (MutationComputedColumn.UUID_COLUMN.equalsIgnoreCase(column.getName())) {
          mutationBld.computedColumn(new MutationComputedColumn(column.getName(), Type.UUID));
          if (pk.isUndefined()) pk = PrimaryKeyMap.of(List.of(i));
        } else if ((column instanceof UnresolvedMetadataColumn) &&
            MutationComputedColumn.TIMESTAMP_METADATA.equalsIgnoreCase(((UnresolvedMetadataColumn) column).getMetadataKey())) {
          mutationBld.computedColumn(new MutationComputedColumn(column.getName(), Type.TIMESTAMP));
        } else {
          inputType.add(field);
        }
      }
      mutationBld.inputDataType(inputType.build());
      mutationBld.outputDataType(outputType.build());
    }
    FlinkConnectorConfig connector = new FlinkConnectorConfig(tableOp.getCatalogTable().getOptions());
    TableAnalysis tableAnalysis = TableAnalysis.of(tableOp.getTableIdentifier(),
        new SourceSinkTableAnalysis(connector, flinkSchema, mutationBld!=null?mutationBld.build():null),
        connector.getTableType(), pk);
    tableLookup.registerTable(tableAnalysis);

    return new AddTableResult(finalTableName, tableOp.getTableIdentifier());
  }

  public ObjectIdentifier createSinkTable(FlinkTableBuilder tableBuilder) {
    return ((CreateTableOperation)executeSqlNode(tableBuilder.buildSql(false))).getTableIdentifier();
  }

  public void insertInto(RelNode relNode, ObjectIdentifier sinkTableId) {
    SqlNode selectQuery = toSqlNode(relNode);
    planBuilder.addInsert(FlinkSqlNodeFactory.createInsert(selectQuery, sinkTableId));
  }

  public SqlOperator lookupUserDefinedFunction(FunctionDefinition fct) {
    String name = getFunctionNameFromClass(fct.getClass()).getDisplay();
    List<SqlOperator> list = new ArrayList<>();
    FlinkPlannerImpl flinkPlanner = this.validatorSupplier.get();
    flinkPlanner.getOrCreateSqlValidator().getOperatorTable()
        .lookupOperatorOverloads(new SqlIdentifier(name, SqlParserPos.ZERO),
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            SqlSyntax.FUNCTION, list, SqlNameMatchers.liberal());
    Preconditions.checkArgument(!list.isEmpty(), "Could not find function: " + name);
    return list.get(0);
  }

  public void addUserDefinedFunction(String name, String clazz, boolean isSystem) {
    SqlCreateFunction functionSql = FlinkSqlNodeFactory.createFunction(name, clazz, isSystem);
    Operation addFctOp = executeSqlNode(functionSql);
    //Function definitions are not in the compiled plan, have to add them explicitly but with fully resolved identifier
    if (addFctOp instanceof CreateCatalogFunctionOperation) {
      functionSql = FlinkSqlNodeFactory.createFunction(FlinkSqlNodeFactory.identifier(
          ((CreateCatalogFunctionOperation) addFctOp).getFunctionIdentifier()), clazz, isSystem);
    }
    planBuilder.addFunction(toSqlString(functionSql));
  }

  private static void checkResultOk(TableResultInternal result) {
    Preconditions.checkArgument(result == TableResultInternal.TABLE_RESULT_OK, "Result is not OK: %s", result);
  }

  private List<RelDataTypeField> convertSchema2RelDataType(Schema schema) {
    List<RelDataTypeField> fields = new ArrayList<>();
    for (int i = 0; i < schema.getColumns().size(); i++) {
      UnresolvedColumn column = schema.getColumns().get(i);
      AbstractDataType type = null;
      if (column instanceof UnresolvedPhysicalColumn) {
        type = ((UnresolvedPhysicalColumn) column).getDataType();
      } else if (column instanceof UnresolvedMetadataColumn) {
        type = ((UnresolvedMetadataColumn) column).getDataType();
      }
      if (type instanceof DataType) {
        fields.add(new RelDataTypeFieldImpl(column.getName(),i,
            typeFactory.createFieldTypeFromLogicalType(((DataType)type).getLogicalType())));
      } else {
        throw new StatementParserException(ErrorLabel.GENERIC, new FileLocation(i, 1),
            "Invalid type: " + column);
      }
    }
    return fields;
  }

  @Value
  public static class ParsedDataType {
    RelDataType relDataType;
    DataType dataType;
  }

  public static final String DUMMY_TABLE_NAME = "__sqrlinternal_types";

  public List<RelDataTypeField> parse2RelDataType(List<ParsedField> fieldList) {
    if (fieldList.isEmpty()) return List.of();
    String createTableStatement = "CREATE TEMPORARY TABLE " + DUMMY_TABLE_NAME + "("
        + fieldList.stream().map(arg -> arg.getName().get() + " " + arg.getType().get())
        .collect(Collectors.joining(",\n")) + ");";
    try {
      CreateTableOperation op = (CreateTableOperation) getOperation(parseSQL(createTableStatement));
      Schema schema = op.getCatalogTable().getUnresolvedSchema();
      return convertSchema2RelDataType(schema);
    } catch (SqlParserException e) {
      FileLocation location = fieldList.get(0).getType().getFileLocation();
      if (e.getCause() instanceof SqlParseException) {
        int fieldPosition = ((SqlParseException) e.getCause()).getPos().getLineNum();
        location = fieldList.get(fieldPosition).getType().getFileLocation();
      }
      throw new StatementParserException(location, e);
    }
  }

  @SneakyThrows
  public Operation executeSQL(String sqlStatement) {
    return executeSqlNode(parseSQL(sqlStatement));
  }

  public Operation executeSqlNode(SqlNode sqlNode) {
    planBuilder.add(sqlNode, this);
    Operation operation = getOperation(sqlNode);
    checkResultOk(tEnv.executeInternal(operation));
    return operation;

  }

  private Operation getOperation(SqlNode sqlNode) {
    return SqlNodeToOperationConversion.convert(validatorSupplier.get(), catalogManager, sqlNode)
        .orElseThrow(() -> new TableException("Unsupported query: " + sqlNode));
  }

  private static List<URL> getUdfUrls(BuildPath buildPath) {
    List<URL> urls = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(buildPath.getUdfPath(), "*.jar")) {
      stream.forEach(p -> {
        try {
          urls.add(p.toUri().toURL());
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (IOException e) {
      //Means there is no lib directory
    }
    return urls;
  }

}
