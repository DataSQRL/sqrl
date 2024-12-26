package com.datasqrl.flinkwrapper;

import com.datasqrl.config.BuildPath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.SqlNodeConvertContext;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;

public class SqrlEnvironment {

  private final StreamTableEnvironmentImpl tEnv;
  private final Supplier<FlinkPlannerImpl> validatorSupplier;
  private final Supplier<CalciteParser> calciteSupplier;
  private final CatalogManager catalogManager;

  @Getter
  private ErrorCollector errors = null;


  public SqrlEnvironment(BuildPath buildPath, ModuleLoader moduleLoader) {
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

  public SqrlEnvironment withErrors(ErrorCollector errors) {
    this.errors = errors;
    return this;
  }


  public SqlNode parseSQL(String sqlStatement) {
    SqlNodeList sqlNodeList = calciteSupplier.get().parseSqlList(sqlStatement);
    List<SqlNode> parsed = sqlNodeList.getList();
    return parsed.get(0);
  }

  public Operation parse(String sqlStatement) {
    SqlNode sqlNode = parseSQL(sqlStatement);
    return tEnv.getParser().parse(sqlStatement).get(0);
  }


  @SneakyThrows
  public void executeSQL(String sqlStatement) {
    //TODO: handle test hints
    System.out.println("SQL: " + sqlStatement);
    tEnv.executeSql(sqlStatement);
  }

  public void executeSqlNode(SqlNode sqlNode) {
    Operation operation = SqlNodeToOperationConversion.convert(validatorSupplier.get(), catalogManager, sqlNode)
            .orElseThrow(() -> new TableException("Unsupported query: " + sqlNode));
    System.out.println(getConvertContext().toQuotedSqlString(sqlNode));
    tEnv.executeInternal(operation);
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
