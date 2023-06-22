/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(VertxExtension.class)
public class FlinkMutationSubscriptionTest extends SubscriptionTest {

  @SneakyThrows
  @Test
  public void runTest() {
    Path rootDir = Path.of("../../sqrl-examples/mutations");

    compile(rootDir, "script.sqrl", "schema.graphqls");
    CompletableFuture<ExecutionResult> fut = executePipeline(rootDir);

    CountDownLatch countDownLatch = new CountDownLatch(3);
    listenOnWebsocket("subscription { receiveEvent { id name } }", (t) -> {
      snapshot.addContent(t.toString());
      countDownLatch.countDown();
    });

    String query = "mutation ($input: GenericEvent!) { createEvent(event: $input) { id } }";
    executeRequests(query, new JsonObject().put("id", "id1").put("name", "name1"));
    executeRequests(query, new JsonObject().put("id", "id2").put("name", "  name2"));
    executeRequests(query, new JsonObject().put("id", "id3").put("name", "  name3   "));

    countDownLatch.await(1, TimeUnit.MINUTES);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);
    snapshot.createOrValidate();
  }
}
