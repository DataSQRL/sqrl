package com.datasqrl.graphql.io;

import java.util.Map;
import java.util.function.Consumer;

public interface SinkConsumer {

  void listen(Consumer<Map<String,Object>> listener, Consumer<Throwable> errorHandler,
      Consumer<Void> endOfStream);

}
