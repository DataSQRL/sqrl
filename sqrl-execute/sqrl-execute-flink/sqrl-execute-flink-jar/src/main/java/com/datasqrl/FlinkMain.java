package com.datasqrl;

import static com.datasqrl.PlanConstants.PLAN_CONFIG;
import static com.datasqrl.PlanConstants.PLAN_JSON;
import static com.datasqrl.PlanConstants.PLAN_SEPARATOR;
import static com.datasqrl.PlanConstants.PLAN_SQL;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.module.resolver.ClasspathResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Used for stand alone flink jars
 */
@Slf4j
public class FlinkMain {

  public static void main(String[] args) {
    ClasspathResourceResolver resourceResolver = new ClasspathResourceResolver();
    log.info("Hello.");
    ErrorCollector errors = ErrorCollector.root();

    Optional<URI> flinkSqlPlan = resourceResolver.resolveFile(NamePath.of("deploy", PLAN_SQL));
    Optional<URI> flinkConfig = resourceResolver.resolveFile(NamePath.of("deploy", PLAN_CONFIG));
    Optional<URI> flinkPlan = resourceResolver.resolveFile(NamePath.of("deploy", PLAN_JSON));

    try {
      (new FlinkMain()).run(errors, flinkPlan, flinkConfig, flinkSqlPlan);
    } catch (Exception e) {
      errors.getCatcher().handle(e);
    }
    if (errors.hasErrors()) {
      log.error(ErrorPrinter.prettyPrint(errors));
    }
  }

  @SneakyThrows
  public TableResult run(ErrorCollector errors, Optional<URI> flinkPlan, Optional<URI> flinkConfig,
      Optional<URI> sqlPlan) {

    Preconditions.checkState(
        flinkPlan.isPresent() || (sqlPlan.isPresent() && flinkConfig.isPresent()),
        "Could not find flink executable plan.");

    Deserializer deserializer = new Deserializer();

    if (sqlPlan.isPresent() && flinkConfig.isPresent()) {
      log.info("Executing sql.");
      URL url = ResourceResolver.toURL(sqlPlan.get());
      String plan;
      try (InputStream in = url.openStream()) {
        plan = IOUtils.toString(in, StandardCharsets.UTF_8);
      }

      Map<String, String> configMap = deserializer.mapYAMLFile(flinkConfig.get(), Map.class);

      String[] commands = plan.split(PLAN_SEPARATOR);

      Configuration sEnvConfig = Configuration.fromMap(configMap);
      StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(
          sEnvConfig);
      EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
          .withConfiguration(Configuration.fromMap(configMap)).build();
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
      TableResult tableResult = null;
      for (String command : commands) {
        String trim = command.trim();
        if (!trim.isEmpty()) {
          tableResult = tEnv.executeSql(trim);
        }
      }
      return tableResult;
    } else {
      FlinkExecutablePlan executablePlan = deserializer.mapJsonFile(flinkPlan.get(),
          FlinkExecutablePlan.class);
      log.info("Found executable.");

      FlinkEnvironmentBuilder builder = new FlinkEnvironmentBuilder(errors);
      StatementSet statementSet = executablePlan.accept(builder, null);
      log.info("Built. " + statementSet);
      TableResult result = statementSet.execute();
      log.info("Plan execution complete: {}", result.getResultKind());
      return result;
    }
  }
}
