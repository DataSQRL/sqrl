/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(VertxExtension.class)
public class MetricsSubscriptionTest extends AbstractSubscriptionTest {

  @SneakyThrows
  @Test
  public void runMetricsMutation() {
    Path rootDir = Path.of("../../sqrl-examples/sensors");

    compile(rootDir,
        rootDir.resolve("metrics-mutation.sqrl"),
        rootDir.resolve("metricsapi.graphqls"));

    CompletableFuture<ExecutionResult> fut = executePipeline(rootDir);

    CountDownLatch countDownLatch = new CountDownLatch(1);
    listenOnWebsocket("subscription HighTempAlert {\n"
        + "  HighTempAlert(sensorid: 1) {\n"
        + "    sensorid\n"
        + "    timeSec\n"
        + "    temp\n"
        + "  }\n"
        + "}", (t) -> {
      snapshot.addContent(t.toString());
      countDownLatch.countDown();
    });

    String query = "mutation AddReading($sensorId: Int!, $temperature: Float!) {\n"
        + "  AddReading(metric: {sensorid: $sensorId, temperature: $temperature}) {\n"
        + "    _source_time\n"
        + "  }\n"
        + "}";
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 41.2), NO_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 45.1), NO_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 55.1), NO_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 65.1), NO_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 50.1), NO_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 62.1), NO_HANDLER);

    countDownLatch.await(1, TimeUnit.MINUTES);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);
    snapshot.createOrValidate();
  }
}
