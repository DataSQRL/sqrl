/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.graphql.AbstractGraphqlTest;
import com.datasqrl.util.data.Banking;
import com.datasqrl.util.data.Sensors;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.datasqrl.util.TestClient.FAIL_HANDLER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class BankingTest extends AbstractGraphqlTest {
  CompletableFuture<ExecutionResult> fut;
  private List<String> events;

  @BeforeEach
  void setUp() {
    fut = execute(Banking.INSTANCE);
    events = new ArrayList<>();
  }

  @SneakyThrows
  @Test
  public void runMetricsMutation() {
    String applicationAlert = "subscription ApplicationAlert {\n"
        + "  ApplicationAlert {\n"
        + "      id\n"
        + "      loan_type_id\n"
        + "      customer_id\n"
        + "      max_amount\n"
        + "      min_amount\n"
        + "      amount\n"
        + "  }\n"
        + "}";

    CountDownLatch countDownLatch = subscribeToAlert(applicationAlert);

    Thread.sleep(5000);

    String addUpdate = "mutation ApplicationUpdate($loan_application_id: Int!, " +
        "  $status: String!, $message: String!) {\n"
        + "  ApplicationUpdates(event: {loan_application_id: $loan_application_id," +
        "     status: $status, message: $message}) {\n"
        + "    loan_application_id\n"
        + "  }\n"
        + "}";

    executeMutation(addUpdate, new JsonObject()
        .put("loan_application_id", 101)
        .put("status", "underwriting")
        .put("message", "The road goes ever on and on"), FAIL_HANDLER);

    Thread.sleep(1000);

    log.debug("count:" + countDownLatch.getCount());
    countDownLatch.await(60, TimeUnit.SECONDS);
    log.debug("count:" + countDownLatch.getCount());

    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);
    validateEvents();
  }
}
