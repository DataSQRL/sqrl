package com.datasqrl.flinkwrapper;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.BuildPath;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkwrapper.analyzer.SQRLLogicalPlanAnalyzer;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.hint.PlannerHints;
import com.datasqrl.flinkwrapper.tables.FlinkConnectorConfigImpl;
import com.datasqrl.flinkwrapper.tables.FlinkTableBuilder;
import com.datasqrl.flinkwrapper.tables.LogEngineTableMetadata;
import com.datasqrl.flinkwrapper.tables.SourceTableAnalysis;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.SqlNodeConvertContext;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;

public class Sqrl2FlinkSQLTranslator implements TableAnalysisLookup {

  public static final String SCHEMA_SUFFIX = "__schema";
  public static final String TABLE_DEFINITION_SUFFIX = "__def";


  private final StreamTableEnvironmentImpl tEnv;
  private final Supplier<FlinkPlannerImpl> validatorSupplier;
  private final Supplier<CalciteParser> calciteSupplier;
  private final CatalogManager catalogManager;

  private final Map<NamePath, SqrlTableMacro> tableMacros = new HashMap<>();
  private final Map<ObjectIdentifier, TableAnalysis> sourceTableMap = new HashMap<>();
  private final HashMultimap<Integer, TableAnalysis> tableMap = HashMultimap.create();

  private final FlinkSqlScriptBuilder sqlScript = new FlinkSqlScriptBuilder();

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
    this.catalogManager = tEnv.getCatalogManager();
    try {
      //TODO: This is a hack - is there a better way to get the calcite parser?
      Field calciteSupplierField = ParserImpl.class.getDeclaredField("calciteParserSupplier");
      calciteSupplierField.setAccessible(true);
      this.calciteSupplier = (Supplier<CalciteParser>) calciteSupplierField.get(tEnv.getParser());
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

  }

  private void registerTable(TableAnalysis tableAnalysis) {
    if (tableAnalysis.isSource()) {
      sourceTableMap.put(tableAnalysis.getIdentifier(), tableAnalysis);
    } else {
      tableMap.put(tableAnalysis.getOriginalRelnode().deepHashCode(), tableAnalysis);
    }
  }

  @Override
  public TableAnalysis lookupSourceTable(ObjectIdentifier objectId) {
    return sourceTableMap.get(objectId);
  }

  @Override
  public Optional<TableAnalysis> lookupTable(RelNode originalRelnode) {
    int hashCode = originalRelnode.deepHashCode();
    return tableMap.get(hashCode).stream().filter(tbl -> tbl.matches(originalRelnode)).findFirst();
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

  @Value
  public static class ViewAnalysis {
    RelNode relNode;
    RelBuilder relBuilder;
    TableAnalysis tableAnalysis;
  }

  public ViewAnalysis analyzeView(SqlNode sqlNode, @Nullable ObjectIdentifier identifier,
      PlannerHints hints, ErrorCollector errors) {
    FlinkPlannerImpl flinkPlanner = this.validatorSupplier.get();

    SqlNode validated = flinkPlanner.validate(sqlNode);
    RowLevelModificationContextUtils.clearContext();
    final SqlNode query;
    if (validated instanceof SqlCreateView) {
      query = ((SqlCreateView) validated).getQuery();
    } else if (validated instanceof SqlAlterViewAs) {
      query = ((SqlAlterViewAs) validated).getNewQuery();
    } else throw new UnsupportedOperationException("Unexpected SQLnode: " + validated);
    RelRoot relRoot = toRelRoot(query, flinkPlanner);
    FlinkRelBuilder relBuilder = getRelBuilder(flinkPlanner);
    SQRLLogicalPlanAnalyzer analyzer = new SQRLLogicalPlanAnalyzer(relRoot.rel, this,
        flinkPlanner.getOrCreateSqlValidator().getCatalogReader().unwrap(CalciteCatalogReader.class),
        relBuilder,
        errors);
    TableAnalysis tableAnalysis = analyzer.analyze(identifier, hints);
    return new ViewAnalysis(tableAnalysis.getCollapsedRelnode(), relBuilder, tableAnalysis);
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
    return
        (FlinkRelBuilder) config.getRelBuilderFactory()
            .create(flinkPlanner.cluster(), null)
            .transform(config.getRelBuilderConfigTransform());
  }

  private SqlNode removeSort(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOrderBy) {
      return ((SqlOrderBy)sqlNode).query;
    }
    return sqlNode;
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
    /* Stage 1: DAG-level rewriting
      In this stage, we try to pull up/out any operators that we want to rewrite as we plan the DAG.
      We attach those to the DAG nodes. Note, that the actual "pulling out" happens during RelNode analysis
      in stage 2. In stage 1, we just finalize the SqlNode that gets passed to Flink.
      Step 1.1: If query has a top level order, we pull it out, so we can later add it to the query if necessary.
     */
    final SqlNode query = removeSort(viewDef instanceof SqlCreateView?
        ((SqlCreateView) viewDef).getQuery():
        ((SqlAlterViewAs)viewDef).getNewQuery());
    final SqlNode rewrittenViewDef = updateViewQuery(query, viewDef);
    // Add the view to Flink using the rewritten SqlNode from stage 1.
    Operation op = executeSqlNode(rewrittenViewDef);
    ObjectIdentifier identifier;
    if (op instanceof AlterViewAsOperation) identifier = ((AlterViewAsOperation) op).getViewIdentifier();
    else if (op instanceof CreateViewOperation) identifier = ((CreateViewOperation) op).getViewIdentifier();
    else throw new UnsupportedOperationException(op.getClass().toString());


    /* Stage 2: Analyze the RelNode/RelRoot
        - pull out top-level sort
      NOTE: Flink modifies the SqlSelect node during validation, so we have to re-create it from the original SQL
     */
    //
    ViewAnalysis viewAnalysis = analyzeView(parseSQL(originalSql), identifier, hints, errors);
    TableAnalysis tableAnalysis = viewAnalysis.getTableAnalysis();
    registerTable(tableAnalysis);
    System.out.println(tableAnalysis);
    System.out.println(identifier.getObjectName() + " PRIMARY KEY: " + (tableAnalysis.getPrimaryKey().isDefined()?tableAnalysis.getPrimaryKey().asSimpleList():""));
    System.out.println(identifier.getObjectName() + " INPUTS: " + (tableAnalysis.getFromTables().stream().map(TableAnalysis::getIdentifier).collect(
        Collectors.toList())));
    System.out.println(identifier.getObjectName() + " PLAN :" + tableAnalysis.getCollapsedRelnode().explain());

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

  public void addTableFunctionInternal(String originalSql, NamePath path, List<FunctionParameter> parameters) {
    //Add to DAG
    SqlNode sqlNode = parseSQL(originalSql);
    ViewAnalysis viewAnalysis = analyzeView(sqlNode, SqlNameUtil.toIdentifier(path),
        PlannerHints.EMPTY, ErrorCollector.root());
    System.out.println("TBL FCT: " + viewAnalysis.relNode.explain());
  }

  public TableAnalysis addImport(String tableName, String tableDefinition,
      Optional<RelDataType> schema,
      Function<FlinkTableBuilder, LogEngineTableMetadata> logEngineBuilder) {
    return addSourceTable(addTable(Optional.of(tableName), tableDefinition, schema, logEngineBuilder));
  }

  public void addGeneratedExport(NamePath exportedTable, ExecutionStage stage, Name sinkName) {
    //TODO: add to dag
  }

  public ObjectIdentifier addExternalExport(String tableName, String tableDefinition, Optional<RelDataType> schema) {
    AddTableResult result = addTable(Optional.of(tableName), tableDefinition, schema, (x) -> {
      throw new UnsupportedOperationException("Export tables require connector configuration");
    });
    return result.baseTableIdentifier;
  }

  public TableAnalysis createTable(String tableDefinition,
      Function<FlinkTableBuilder, LogEngineTableMetadata> logEngineBuilder) {
    AddTableResult result = addTable(Optional.empty(), tableDefinition, Optional.empty(), logEngineBuilder);
    return addSourceTable(result);
  }

  private TableAnalysis addSourceTable(AddTableResult addResult) {
    SqlCreateView view = FlinkSqlNodeFactory.createView(addResult.tableName, FlinkSqlNodeFactory.selectAllFromTable(
        FlinkSqlNodeFactory.identifier(addResult.baseTableIdentifier)));
    return addView(toSqlString(view), PlannerHints.EMPTY, ErrorCollector.root());
  }

  @Value
  private static class AddTableResult {
    String tableName;
    ObjectIdentifier baseTableIdentifier;
  }

  private AddTableResult addTable(Optional<String> tableName, String createTableSql,
      Optional<RelDataType> schema, Function<FlinkTableBuilder, LogEngineTableMetadata> logEngineBuilder) {
    SqlNode tableSqlNode = parseSQL(createTableSql);
    Preconditions.checkArgument(tableSqlNode instanceof SqlCreateTable, "Expected CREATE TABLE statement");
    SqlCreateTable tableDefinition = (SqlCreateTable) tableSqlNode;
    SqlCreateTable fullTable = tableDefinition;
    final String finalTableName = tableName.orElse(tableDefinition.getTableName().getSimple());
    final String baseTableName = finalTableName+TABLE_DEFINITION_SUFFIX;
    if (schema.isPresent()) {
      //Use LIKE to merge schema with table definition
      String schemaTableName = finalTableName+SCHEMA_SUFFIX;
      SqlCreateTable schemaTable = FlinkSqlNodeFactory.createTable(schemaTableName, schema.get());
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
    LogEngineTableMetadata createMedata = null;
    if (fullTable.getPropertyList().isEmpty()) { //it's an internal CREATE TABLE for a mutation
      FlinkTableBuilder tableBuilder = FlinkTableBuilder.toBuilder(fullTable);
      tableBuilder.setName(baseTableName);
      createMedata = logEngineBuilder.apply(tableBuilder);
      //TODO: plan operation but don't execute it so we can get the schema, then pull out computed columns and make those "fixed"
      //since they will be computed on the server
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
    FlinkConnectorConfigImpl connector = new FlinkConnectorConfigImpl(tableOp.getCatalogTable().getOptions());
    TableAnalysis tableAnalysis = TableAnalysis.of(tableOp.getTableIdentifier(),
        new SourceTableAnalysis(connector, flinkSchema, createMedata),
        connector.getTableType(), pk);
    registerTable(tableAnalysis);
    System.out.println(tableAnalysis);

    return new AddTableResult(finalTableName, tableOp.getTableIdentifier());
  }


  public void addFunction(String name, String clazz) {
    executeSqlNode(FlinkSqlNodeFactory.createFunction(name, clazz));
  }


  private static void checkResultOk(TableResultInternal result) {
    Preconditions.checkArgument(result == TableResultInternal.TABLE_RESULT_OK, "Result is not OK: %s", result);
  }

  @SneakyThrows
  public TableResult executeSQL(String sqlStatement) {
    //TODO: handle test hints
    System.out.println("SQL: " + sqlStatement);
    return tEnv.executeSql(sqlStatement);
  }

  public Operation executeSqlNode(SqlNode sqlNode) {
    System.out.println("SQL: " + getConvertContext().toQuotedSqlString(sqlNode));
    Operation operation = SqlNodeToOperationConversion.convert(validatorSupplier.get(), catalogManager, sqlNode)
            .orElseThrow(() -> new TableException("Unsupported query: " + sqlNode));
    executeOperation(operation);
    return operation;
  }

  public void executeOperation(Operation operation) {
    checkResultOk(tEnv.executeInternal(operation));
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
