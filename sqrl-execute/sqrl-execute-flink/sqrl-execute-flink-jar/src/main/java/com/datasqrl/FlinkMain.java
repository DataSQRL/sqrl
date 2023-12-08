package com.datasqrl;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.serializer.Deserializer;
import com.google.common.io.Resources;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

/**
 * Used for stand alone flink jars
 */
@Slf4j
public class FlinkMain {
  public static void main(String[] args) {

    (new FlinkMain()).run();
  }

  @SneakyThrows
  public void run() {
    log.info("Hello.");

    URI flinkPlan = Resources.getResource("build/deploy/flink-plan.json").toURI();

    Deserializer deserializer = new Deserializer();
    FlinkExecutablePlan executablePlan = deserializer.mapJsonFile(flinkPlan, FlinkExecutablePlan.class);
    log.info("Found executable.");

    ErrorCollector errors = ErrorCollector.root();
    try {
      FlinkEnvironmentBuilder builder = new FlinkEnvironmentBuilder(errors);
      StatementSet statementSet = executablePlan.accept(builder, null);
      log.info("Built. " + statementSet);
      TableResult result = statementSet.execute();
      log.info("Plan execution complete: {}", result.getResultKind());
    } catch (Exception e) {
      errors.getCatcher().handle(e);
    }
    if (errors.hasErrors()) {
      log.error(ErrorPrinter.prettyPrint(errors));
    }
  }
}
