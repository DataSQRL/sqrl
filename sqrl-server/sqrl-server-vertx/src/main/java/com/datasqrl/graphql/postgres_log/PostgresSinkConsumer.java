package com.datasqrl.graphql.postgres_log;

import com.datasqrl.graphql.io.SinkConsumer;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class PostgresSinkConsumer implements SinkConsumer {

  private PostgresListenNotifyConsumer consumer;

  @SneakyThrows
  @Override
  public void listen(Consumer<Object> listener, Consumer<Throwable> errorHandler, Consumer<Void> endOfStream) {
    consumer.subscribe(listener::accept);
  }
}
