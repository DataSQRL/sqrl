/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.engine.ExecutionResult;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class BankingTest extends AbstractSubscriptionTest {

  @SneakyThrows
  @Test
  public void runMetricsMutation() {
    Path rootDir = Path.of("../../sqrl-examples/banking");

    compile(rootDir,
            rootDir.resolve("loan.sqrl"),
            rootDir.resolve("loan.graphqls"));

    CompletableFuture<ExecutionResult> fut = executePipeline(rootDir);

    CountDownLatch countDownLatch = new CountDownLatch(1);
    listenOnWebsocket("subscription ApplicationAlert {\n"
            + "  ApplicationAlert {\n"
            + "      id\n"
            + "      loan_type_id\n"
            + "      customer_id\n"
            + "      max_amount\n"
            + "      min_amount\n"
            + "      amount\n"
            + "  }\n"
            + "}", (t) -> {
      log.info("Got subscription");
      snapshot.addContent(t.toString());
      countDownLatch.countDown();
    }).future().toCompletionStage().toCompletableFuture().exceptionally((e)->fail()).get();

    Thread.sleep(10000);

//    executeRequests("mutation de {\n" +
//            "  Applications(event:{\n" +
//            "    id:102\n" +
//            "    duration:50\n" +
//            "    amount:1\n" +
//            "    loan_type_id:8\n" +
//            "    customer_id: 1\n" +
//            "    application_date: \"2023-05-19T03:34:22.922Z\"\n" +
////            "    updated_at: \"2023-05-19T03:34:22.922Z\"\n" +
//            "  }) {\n" +
//            "    id\n" +
//            "  }\n" +
//            "}", new JsonObject(), FAIL_HANDLER);


    String query = "mutation ApplicationUpdate($loan_application_id: Int!, " +
            "  $status: String!, $message: String!) {\n"
            + "  ApplicationUpdates(event: {loan_application_id: $loan_application_id," +
            "     status: $status, message: $message}) {\n"
            + "    loan_application_id\n"
            + "  }\n"
            + "}";
//  {"loan_application_id":1,"status":"underwriting","message":"The road goes ever on and on","timestamp":"2023-06-15T19:08:34.610Z"}

    executeRequests(query, new JsonObject()
            .put("loan_application_id", 101)
            .put("status", "underwriting")
            .put("message", "The road goes ever on and on")
            , FAIL_HANDLER);
    Thread.sleep(1000);

    log.info("count:" + countDownLatch.getCount());
    countDownLatch.await(60, TimeUnit.SECONDS);
    log.info("count:" + countDownLatch.getCount());

    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);
    snapshot.createOrValidate();
  }
}
