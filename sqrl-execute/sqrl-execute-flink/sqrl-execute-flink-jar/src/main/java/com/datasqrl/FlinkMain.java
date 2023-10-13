package com.datasqrl;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.resolver.ClasspathResourceResolver;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Arrays;
import java.util.Base64;
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
    ClasspathResourceResolver resourceResolver = new ClasspathResourceResolver();

    Preconditions.checkState(args.length == 1, "Requires a base64 flink plan as argument");
    log.info("Files:" + resourceResolver.getFiles());
    (new FlinkMain()).run(resourceResolver, args[0]);
  }

  @SneakyThrows
  public void run(ResourceResolver resourceResolver, String arg) {
    log.info("Hello.");
    String flinkPlan = new String(Base64.getDecoder().decode(arg.getBytes()));
    Preconditions.checkState(!flinkPlan.isEmpty(), "Could not find flink executable plan.");

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
