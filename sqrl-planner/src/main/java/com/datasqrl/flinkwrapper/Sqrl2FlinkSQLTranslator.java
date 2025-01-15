package com.datasqrl.flinkwrapper;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.BuildPath;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkwrapper.tables.FlinkTableBuilder;
import com.google.common.base.Preconditions;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.SqlNodeConvertContext;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;

public class Sqrl2FlinkSQLTranslator {

  public static final String SCHEMA_SUFFIX = "__schema";
  public static final String EXPORT_SUFFIX = "__export";


  private final StreamTableEnvironmentImpl tEnv;
  private final Supplier<FlinkPlannerImpl> validatorSupplier;
  private final Supplier<CalciteParser> calciteSupplier;
  private final CatalogManager catalogManager;

  private final AtomicInteger exportTableCounter = new AtomicInteger(0);
  private final Map<NamePath, SqrlTableMacro> tableMacros = new HashMap<>();


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

  public RelRoot toRelRoot(SqlNode sqlNode) {
    FlinkPlannerImpl flinkPlanner = this.validatorSupplier.get();
    SqlNode validated = flinkPlanner.validate(sqlNode);
    RowLevelModificationContextUtils.clearContext();
    final SqlNode query;
    if (validated instanceof SqlCreateView) {
      query = ((SqlCreateView) validated).getQuery();
    } else if (validated instanceof SqlAlterViewAs) {
      query = ((SqlAlterViewAs) validated).getNewQuery();
    } else throw new UnsupportedOperationException("Unexpected SQLnode: " + validated);
    SqlNodeConvertContext context = new SqlNodeConvertContext(flinkPlanner, catalogManager);
    SqlNode validatedQuery = context.getSqlValidator().validate(query);
    return context.toRelRoot(validatedQuery);
  }

  private SqlNode removeSort(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOrderBy) {
      return ((SqlOrderBy)sqlNode).query;
    }
    return sqlNode;
  }

  public void addView(SqlNode tableDef, String originalSql, ErrorCollector errors) {
    Preconditions.checkArgument(tableDef instanceof SqlCreateView || tableDef instanceof SqlAlterViewAs,
        "Unexpected table definition: " + tableDef);
    //If the query has a top level order, we need to pull it out first
    final SqlNode queryWithoutSort;
    if (tableDef instanceof SqlCreateView) {
      SqlCreateView viewDef = (SqlCreateView) tableDef;
      SqlNode query = removeSort(viewDef.getQuery());
      queryWithoutSort = query==viewDef.getQuery()?viewDef:
          new SqlCreateView(viewDef.getParserPosition(),
          viewDef.getViewName(), viewDef.getFieldList(), query, viewDef.getReplace(),
          viewDef.isTemporary(), viewDef.isIfNotExists(),
          viewDef.getComment().orElse(null), viewDef.getProperties().orElse(null));
    } else {
      SqlAlterViewAs alterDef = (SqlAlterViewAs) tableDef;
      SqlNode query = removeSort(alterDef.getNewQuery());
      queryWithoutSort = query==alterDef.getNewQuery()?alterDef:
          new SqlAlterViewAs(alterDef.getParserPosition(), alterDef.getViewIdentifier(), query);
    }

    //Analyze relRoot
    //Flink modifies the SqlSelect node during validation, so we have to re-create it from the original SQL
    RelRoot relRoot = toRelRoot(parseSQL(originalSql));
    System.out.println(relRoot.rel);


    //Create DAG node



    executeSqlNode(queryWithoutSort);
  }

  public void addRelationship() {

  }

  public void addTableFunction() {

  }

  private void addTableFunctionInternal() {
    //add to internal table function map
  }

  public void addImport(String tableName, SqlCreateTable tableDefinition, Optional<RelDataType> schema) {
    CreateTableOperation tableOp = addTable(Optional.of(tableName), tableDefinition, schema);
    addSourceTable(tableOp, false);
  }

  public void addGeneratedExport(NamePath exportedTable, ExecutionStage stage, Name sinkName) {
    //TODO: add to dag
  }

  public void addExternalExport(NamePath exportedTable, SqlCreateTable tableDefinition, Optional<RelDataType> schema) {
    String exportTableName = exportedTable.getLast().getDisplay() + EXPORT_SUFFIX + exportTableCounter.incrementAndGet();
    CreateTableOperation tableOp = addTable(Optional.of(exportTableName), tableDefinition, schema);
    //TODO: add to dag
  }

  public void createTable(SqlCreateTable tableDefinition) {
    CreateTableOperation tableOp = addTable(Optional.empty(), tableDefinition, Optional.empty());
    addSourceTable(tableOp, tableDefinition.getPropertyList().isEmpty());
  }

  private void addSourceTable(CreateTableOperation tableOp, boolean isMutation) {
    //TODO: add to dag: determine table type, pull out pk & timestamp

  }

  private CreateTableOperation addTable(Optional<String> tableName, SqlCreateTable tableDefinition, Optional<RelDataType> schema) {
    SqlCreateTable fullTable = tableDefinition;
    if (schema.isPresent()) {
      //Use LIKE to merge schema with table definition
      String schemaTableName = tableName.orElse(tableDefinition.getTableName().getSimple())+SCHEMA_SUFFIX;
      SqlCreateTable schemaTable = FlinkSqlNodeFactory.createTable(schemaTableName, schema.get());
      executeSqlNode(schemaTable);

      SqlTableLike likeClause = new SqlTableLike(SqlParserPos.ZERO,
          FlinkSqlNodeFactory.identifier(schemaTableName),
          List.of());
      fullTable = new SqlCreateTableLike(tableDefinition.getParserPosition(),
          tableName.map(FlinkSqlNodeFactory::identifier).orElse(tableDefinition.getTableName()),
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
          FlinkSqlNodeFactory.identifier(tableName.get()),
          tableDefinition.getColumnList(),
          tableDefinition.getTableConstraints(),
          tableDefinition.getPropertyList(),
          tableDefinition.getPartitionKeyList(),
          tableDefinition.getWatermark().orElse(null),
          tableDefinition.getComment().orElse(null),
          tableDefinition.isTemporary(),
          tableDefinition.ifNotExists);
    }
    if (fullTable.getPropertyList().isEmpty()) { //it's an internal CREATE TABLE for a mutation
      FlinkTableBuilder tableBuilder = FlinkTableBuilder.toBuilder(fullTable);
      //TODO: get connector config from log engine, check for event-key and event-time
      tableBuilder.setConnectorOptions(Map.of("connector","datagen"));
      //TODO: if primary key is enforced make it an upsert stream
      fullTable = tableBuilder.buildSql(false);
    }
    CreateTableOperation tableOp = (CreateTableOperation)executeSqlNode(fullTable);
    return tableOp;
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
