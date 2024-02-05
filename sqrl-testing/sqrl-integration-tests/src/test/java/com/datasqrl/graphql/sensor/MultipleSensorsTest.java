/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.sensor;

import com.datasqrl.util.data.Sensors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MiniClusterExtension.class)
public class MultipleSensorsTest extends SensorsTest {

  @SneakyThrows
  @Test
  public void multipleSubscribersTest() {
    fut = execute(Sensors.INSTANCE_MUTATION);
    events = new ArrayList<>();
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
