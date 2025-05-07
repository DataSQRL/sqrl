package com.datasqrl.graphql.io;

import java.util.function.Consumer;

/**
 * Consuming records from a sink (kafka topic or postGreSQL table). It is a sink from the SQRL pipeline perspective.
 */
public interface SinkConsumer {

  void listen(Consumer<Object> listener, Consumer<Throwable> errorHandler,
      Consumer<Void> endOfStream);

}
