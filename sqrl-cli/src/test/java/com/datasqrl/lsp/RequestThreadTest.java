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
package com.datasqrl.lsp;

import static com.datasqrl.lsp.TestLanguageServers.TIMEOUT_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RequestThreadTest {

  private final RequestThread requests = new RequestThread();

  @AfterEach
  void close() {
    requests.close();
  }

  @Test
  void givenChangesAndRequests_whenHandled_thenInTheOrderTheyArrived() throws Exception {
    var handled = new CopyOnWriteArrayList<String>();

    requests.handle(() -> handled.add("change 1"));
    var answer = requests.answer(() -> handled.add("request"));
    requests.handle(() -> handled.add("change 2"));
    requests.answer(() -> true).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(answer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    assertThat(handled).containsExactly("change 1", "request", "change 2");
  }

  @Test
  void givenRequestCancelledBeforeItsTurn_whenItsTurnComes_thenItIsSkipped() throws Exception {
    var busy = new CountDownLatch(1);
    requests.handle(() -> await(busy));
    var answered = new AtomicBoolean();

    var request = requests.answer(() -> answered.getAndSet(true));
    // As LSP4J does when the editor cancels the request
    request.cancel(true);
    busy.countDown();
    requests.answer(() -> true).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(answered).isFalse();
  }

  @Test
  void givenNotificationThatFails_whenHandled_thenTheNextMessagesAreStillHandled()
      throws Exception {
    requests.handle(
        () -> {
          throw new IllegalStateException("A bug in a notification");
        });

    assertThat(requests.answer(() -> "answer").get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
        .isEqualTo("answer");
  }

  @Test
  void givenAnswerThatCompletesLater_whenAnswered_thenItIsThatAnswer() throws Exception {
    var later = new CompletableFuture<List<String>>();

    var answer = requests.answerLater(() -> later);
    later.complete(List.of("after the check"));

    assertThat(answer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).containsExactly("after the check");
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
