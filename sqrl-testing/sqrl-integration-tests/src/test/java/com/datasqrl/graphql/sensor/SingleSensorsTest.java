/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.sensor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.graphql.AbstractGraphqlTest;
import com.datasqrl.util.data.Sensors;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MiniClusterExtension.class)
public class SingleSensorsTest extends SensorsTest {

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
