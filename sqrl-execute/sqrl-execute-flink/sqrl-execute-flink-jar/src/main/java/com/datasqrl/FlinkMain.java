package com.datasqrl;

import static com.datasqrl.PlanConstants.PLAN_CONFIG;
import static com.datasqrl.PlanConstants.PLAN_SEPARATOR;
import static com.datasqrl.PlanConstants.PLAN_SQL;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.resolver.ClasspathResourceResolver;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.ConfigConstants;
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

    (new FlinkMain()).run(resourceResolver);
  }

  @SneakyThrows
  public void run(ResourceResolver resourceResolver) {
    log.info("Hello.");

    Optional<URI> flinkSqlPlan = resourceResolver.resolveFile(NamePath.of("deploy", "flink-plan.sql"));
    Optional<URI> flinkConfig = resourceResolver.resolveFile(NamePath.of("deploy", PLAN_CONFIG));

    Optional<URI> flinkPlan = resourceResolver.resolveFile(NamePath.of("deploy", PLAN_SQL));
    Preconditions.checkState(flinkPlan.isPresent(), "Could not find flink executable plan.");

    Deserializer deserializer = new Deserializer();
    FlinkExecutablePlan executablePlan = deserializer.mapJsonFile(flinkPlan.get(), FlinkExecutablePlan.class);
    log.info("Found executable.");

    ErrorCollector errors = ErrorCollector.root();
    try {
      if (flinkSqlPlan.isPresent() && flinkConfig.isPresent()) {
        log.info("Executing sql.");
        URL url = ResourceResolver.toURL(flinkSqlPlan.get());
        String plan;
        try (InputStream in = url.openStream()){
          plan = IOUtils.toString(in, StandardCharsets.UTF_8);
        }

        Map<String, String> configMap = deserializer.mapYAMLFile(flinkConfig.get(), Map.class);

        String[] commands = plan.split(PLAN_SEPARATOR);

        Configuration sEnvConfig = Configuration.fromMap(configMap);
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(sEnvConfig);
        EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
            .withConfiguration(Configuration.fromMap(configMap)).build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

        for (String command : commands) {
          String trim = command.trim();
          if (!trim.isEmpty()) {
            tEnv.executeSql(trim);
          }
        }
      } else {
        FlinkEnvironmentBuilder builder = new FlinkEnvironmentBuilder(errors);
        StatementSet statementSet = executablePlan.accept(builder, null);
        log.info("Built. " + statementSet);
        TableResult result = statementSet.execute();
        log.info("Plan execution complete: {}", result.getResultKind());
      }
    } catch (Exception e) {
      errors.getCatcher().handle(e);
    }
    if (errors.hasErrors()) {
      log.error(ErrorPrinter.prettyPrint(errors));
    }
  }
}
