/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.io.SinkConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class KafkaSinkConsumer<IN> implements SinkConsumer {

  final KafkaConsumer<String, IN> consumer;

  @Override
  public void listen(
      Consumer<Object> listener, Consumer<Throwable> errorHandler, Consumer<Void> endOfStream) {
    consumer
        .handler(
            kafkaRecord -> {
              try {
                Object result = kafkaRecord.value();
                listener.accept(result);
              } catch (Exception e) {
                errorHandler.accept(e);
              }
            })
        .exceptionHandler(errorHandler::accept)
        .endHandler(endOfStream::accept);
  }
}
