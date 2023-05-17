package com.datasqrl.graphql.server;

import java.util.concurrent.CompletableFuture;

public abstract class SinkEmitter {
  public abstract CompletableFuture<SinkResult> send(SinkRecord data);
}
