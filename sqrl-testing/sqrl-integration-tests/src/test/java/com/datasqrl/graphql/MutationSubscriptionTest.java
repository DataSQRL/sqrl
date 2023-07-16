/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import static com.datasqrl.util.TestClient.NO_HANDLER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class MutationSubscriptionTest extends AbstractGraphqlTest {

  @SneakyThrows
  @Test
  public void runMutationTest() {
    CompletableFuture<ExecutionResult> fut = execute("../../sqrl-examples/mutations");

    CountDownLatch countDownLatch = new CountDownLatch(3);
    final List<String> events = new ArrayList<>();
    client.listen("subscription { receiveEvent { id name } }", (t) -> {
      events.add(t.toString());
      countDownLatch.countDown();
    }).future().toCompletionStage().toCompletableFuture().get();

    String query = "mutation ($input: GenericEvent!) { createEvent(event: $input) { id } }";
    client.query(query, new JsonObject().put("input", new JsonObject().put("id", "id1").put("name", "name1")), NO_HANDLER);
    client.query(query, new JsonObject().put("input", new JsonObject().put("id", "id2").put("name", "  name2")), NO_HANDLER);
    client.query(query, new JsonObject().put("input", new JsonObject().put("id", "id3").put("name", "  name3   ")), NO_HANDLER);

    countDownLatch.await(200, TimeUnit.SECONDS);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);
    Collections.sort(events);
    snapshot.addContent(String.join("\n", events))
            .createOrValidate();
  }
}
