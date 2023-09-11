/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.graphql.AbstractGraphqlTest;
import com.datasqrl.util.data.Conference;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
public class ConferenceTest extends AbstractGraphqlTest {
  CompletableFuture<ExecutionResult> fut;

  @BeforeEach
  void setUp() {
    fut = execute(Conference.INSTANCE);
    events = new ArrayList<>();
  }

  @SneakyThrows
  @Test
//  @Disabled
  public void run() {
    CountDownLatch countDownLatch = new CountDownLatch(1);

    Thread.sleep(1000);
    executeQuery("query {"
        + "EventSearch(query: \"test\", afterTime: \"0\") {"
        + " title"
        + "}"
        + "}", null, new Consumer<HttpResponse<JsonObject>>() {
      @Override
      public void accept(HttpResponse<JsonObject> jsonObjectHttpResponse) {
        System.out.println(jsonObjectHttpResponse);
        countDownLatch.countDown();
      }
    });
    countDownLatch.await();
  }
}
