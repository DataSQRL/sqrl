/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.util.data.Sensors;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.datasqrl.util.TestClient.NO_HANDLER;
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

  @BeforeEach
  void setUp() {
    fut = execute(Sensors.INSTANCE_MUTATION);
    events = new ArrayList<>();
  }

  @SneakyThrows
  @Test
  public void singleSubscriptionMutationTest() {
    CountDownLatch countDownLatch = subscribeToAlert(alert);

    Thread.sleep(1000);

    executeMutation(addReading, triggerAlertJson);

    countDownLatch.await(120, TimeUnit.SECONDS);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);

    validateEvents();
  }

  @SneakyThrows
  @Test
  public void multipleSubscribersTest() {
    List<CountDownLatch> latches = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      latches.add(subscribeToAlert(alert));
    }

    Thread.sleep(1000);

    executeMutation(addReading, triggerAlertJson);

    // Wait a long time for the first latch, the rest should complete soon after
    int timeout = 120;
    for (CountDownLatch latch : latches) {
      latch.await(timeout, TimeUnit.SECONDS);
      timeout = timeout / 2;
    }

    fut.cancel(true);

    validateEvents();
  }
}
