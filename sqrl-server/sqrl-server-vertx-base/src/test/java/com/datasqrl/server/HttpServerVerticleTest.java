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
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.server.config.ServerConfig;
import io.vertx.core.Promise;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class HttpServerVerticleTest {

  @Test
  void stop_closesRemainingResourcesAfterCloseFailure() throws Exception {
    var verticle = new HttpServerVerticle(Path.of("."), new ServerConfig(), Map.of());
    var closeFailure = new IllegalStateException("first close failed");
    var secondResourceClosed = new AtomicBoolean();
    closeablesOf(verticle)
        .add(
            () -> {
              throw closeFailure;
            });
    closeablesOf(verticle).add(() -> secondResourceClosed.set(true));
    var stopPromise = Promise.<Void>promise();

    verticle.stop(stopPromise);

    assertThatThrownBy(() -> stopPromise.future().toCompletionStage().toCompletableFuture().get())
        .hasCause(closeFailure);
    assertThat(secondResourceClosed).isTrue();
  }

  @SuppressWarnings("unchecked")
  private static List<AutoCloseable> closeablesOf(HttpServerVerticle verticle)
      throws ReflectiveOperationException {
    var field = HttpServerVerticle.class.getDeclaredField("closeables");
    field.setAccessible(true);
    return (List<AutoCloseable>) field.get(verticle);
  }
}
