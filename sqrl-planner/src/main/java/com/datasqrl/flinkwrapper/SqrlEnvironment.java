package com.datasqrl.flinkwrapper;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.BuildPath;
import com.datasqrl.engine.log.Log;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.flinkwrapper.parser.SqrlStatement;
import com.datasqrl.flinkwrapper.parser.SqrlStatement.Type;
import com.datasqrl.flinkwrapper.parser.SqrlStatementParser;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.util.CheckUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.net.URL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;

public class SqrlEnvironment {

  private final StreamTableEnvironmentImpl tEnv;
  private final Supplier<FlinkPlannerImpl> validatorSupplier;
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

  }

  public SqrlEnvironment withErrors(ErrorCollector errors) {
    this.errors = errors;
    return this;
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
