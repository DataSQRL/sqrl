package com.datasqrl;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.net.URI;
import java.net.URISyntaxException;
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
    Optional<URI> flinkPlan = Optional.ofNullable(Resources.getResource("build/deploy/flink-plan.json"))
        .map(u-> {
          try {
            return u.toURI();
          } catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
        });
    Preconditions.checkState(flinkPlan.isPresent(), "Could not find flink executable plan.");

    Deserializer deserializer = new Deserializer();
    FlinkExecutablePlan executablePlan = deserializer.mapJsonFile(flinkPlan.get(), FlinkExecutablePlan.class);
    log.info("Found executable.");

    ErrorCollector errors = ErrorCollector.root();
    try {
      FlinkEnvironmentBuilder builder = new FlinkEnvironmentBuilder(errors);
      StatementSet statementSet = executablePlan.accept(builder, null);
      log.info("Built." + statementSet);
      TableResult result = statementSet.execute();
      log.info("result." + result);

      log.info("Plan execution complete: {}", result.getResultKind());
    } catch (Exception e) {
      e.printStackTrace();
      errors.getCatcher().handle(e);
    }
    System.out.println(ErrorPrinter.prettyPrint(errors));
  }
}
