package com.datasqrl.graphql.io;

import java.util.function.Consumer;

public interface SinkConsumer {

  void listen(Consumer<Object> listener, Consumer<Throwable> errorHandler,
      Consumer<Void> endOfStream);

}
