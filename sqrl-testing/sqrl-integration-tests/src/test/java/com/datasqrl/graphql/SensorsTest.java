/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.util.data.Sensors;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SensorsTest extends AbstractGraphqlTest {
  CompletableFuture<ExecutionResult> fut;

  String alert = "subscription HighTempAlert {\n"
      + "  HighTempAlert(sensorid: 1) {\n"
      + "    sensorid\n"
      + "    temp\n"
      + "  }\n"
      + "}";
  String addReading = "mutation AddReading($sensorId: Int!, $temperature: Float!) {\n"
      + "  AddReading(metric: {sensorid: $sensorId, temperature: $temperature}) {\n"
      + "    _source_time\n"
      + "  }\n"
      + "}";

  JsonObject triggerAlertJson = new JsonObject().put("sensorId", 1).put("temperature", 62.1);
  JsonObject nontriggerAlertJson = new JsonObject().put("sensorId", 2).put("temperature", 62.1);

  @SneakyThrows
  @Test
  public void singleSubscriptionMutationTest() {
    fut = execute(Sensors.INSTANCE_MUTATION);
    events = new ArrayList<>();
    Thread.sleep(5000);

    CountDownLatch countDownLatch = subscribeToAlert(alert);

    Thread.sleep(1000);

    executeMutation(addReading, nontriggerAlertJson);//test subscription filtering
    executeMutation(addReading, triggerAlertJson);

    countDownLatch.await(120, TimeUnit.SECONDS);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);

    validateEvents();
  }
}
