/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import io.vertx.core.json.JsonObject;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class MutationSubscriptionTest extends AbstractSubscriptionTest {

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
            + "    temp\n"
            + "  }\n"
            + "}", (t) -> {
      snapshot.addContent(t.toString());
      countDownLatch.countDown();
    }).future().toCompletionStage().toCompletableFuture().get();

    Thread.sleep(1000);

    String query = "mutation AddReading($sensorId: Int!, $temperature: Float!) {\n"
            + "  AddReading(metric: {sensorid: $sensorId, temperature: $temperature}) {\n"
            + "    _source_time\n"
            + "  }\n"
            + "}";
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 41.2), FAIL_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 45.1), FAIL_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 55.1), FAIL_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 65.1), FAIL_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 50.1), FAIL_HANDLER);
    executeRequests(query, new JsonObject().put("sensorId", 1).put("temperature", 62.1), FAIL_HANDLER);

    countDownLatch.await(120, TimeUnit.SECONDS);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);
    snapshot.createOrValidate();
  }

  @SneakyThrows
  @Test
  public void runMutationTest() {
    Path rootDir = Path.of("../../sqrl-examples/mutations");

    compile(rootDir);

    CompletableFuture<ExecutionResult> fut = executePipeline(rootDir);

    CountDownLatch countDownLatch = new CountDownLatch(3);
    listenOnWebsocket("subscription { receiveEvent { id name } }", (t) -> {
      snapshot.addContent(t.toString());
      countDownLatch.countDown();
    }).future().toCompletionStage().toCompletableFuture().get();

    String query = "mutation ($input: GenericEvent!) { createEvent(event: $input) { id } }";
    executeRequests(query, new JsonObject().put("input", new JsonObject().put("id", "id1").put("name", "name1")), FAIL_HANDLER);
    executeRequests(query, new JsonObject().put("input", new JsonObject().put("id", "id2").put("name", "  name2")), FAIL_HANDLER);
    executeRequests(query, new JsonObject().put("input", new JsonObject().put("id", "id3").put("name", "  name3   ")), FAIL_HANDLER);

    countDownLatch.await(200, TimeUnit.SECONDS);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);
    snapshot.createOrValidate();
  }
}
