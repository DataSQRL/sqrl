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
package com.datasqrl.server.kafka;

import com.datasqrl.server.io.SinkProducer;
import com.datasqrl.server.io.SinkResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SaslAuthenticationException;

@RequiredArgsConstructor
public class SaslRetryingSinkProducer implements SinkProducer {

  private final Vertx vertx;
  private final SinkProducer delegate;
  private final Duration delay;

  @Override
  public Future<SinkResult> send(Record record) {
    return delegate
        .send(record)
        .recover(
            e ->
                isSaslAuthFailure(e)
                    ? vertx.timer(delay).compose(v -> delegate.send(record))
                    : Future.failedFuture(e));
  }

  public static boolean isSaslAuthFailure(Throwable e) {
    return ExceptionUtils.indexOfType(e, SaslAuthenticationException.class) >= 0;
  }
}
