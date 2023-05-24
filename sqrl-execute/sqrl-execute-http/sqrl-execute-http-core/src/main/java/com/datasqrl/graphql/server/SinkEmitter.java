package com.datasqrl.graphql.server;

import io.vertx.core.Promise;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class SinkEmitter {
  public abstract CompletableFuture<SinkResult> send(SinkRecord data, Promise<Object> fut,
      Map<String, Object> entry);
}
