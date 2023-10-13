/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import static com.datasqrl.util.TestClient.FAIL_HANDLER;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.graphql.AbstractGraphqlTest;
import com.datasqrl.util.data.Conference;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
  public void run() {
    CountDownLatch countDownLatch = new CountDownLatch(1);

    executeMutation("mutation {\n"
        + "  Likes(liked: {\n"
        + "    eventId: 1136937\n"
        + "    userid: \"10\"\n"
        + "    liked: true\n"
        + "  }) {\n"
        + "    userid\n"
        + "  }\n"
        + "}", null, FAIL_HANDLER);
    executeMutation("mutation {\n"
        + "AddInterest(interest:{\n"
        + "  userid:\"10\",\n"
        + "  text:\"ai\"\n"
        + "}) {\n"
        + "  userid\n"
        + "}\n"
        + "}", null, FAIL_HANDLER);

    executeQueryUntilTrue("query {\n"
        + "\n"
        + "  RecommendedEvents(userid:\"10\") {\n"
        + "    url\n"
        + "    title\n"
        + "    score\n"
        + "  }\n"
        + "  \n"
        + " \n"
        + "}", null,
        resp -> {
          countDownLatch.countDown();
          events.add(resp.body().encode());
        },
        resp -> {
          JsonObject body = resp.body();
          log.info("Got response " + body.encode());
          if (resp.statusCode() != 200) {
            fail(String.format("Non 200 response. %s", resp.body().encode()));
          }
          JsonArray jsonArray = resp.body().getJsonObject("data").getJsonArray("RecommendedEvents");
          return !jsonArray.isEmpty();
        });
    countDownLatch.await();
    validateEvents();
  }

  @AfterEach
  public void teardown() {
    snapshot.createOrValidate();
  }
}
