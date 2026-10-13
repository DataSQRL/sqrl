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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * The thread that handles the messages of the editor in the order they arrive, so that each answer
 * is about the documents as the editor sent them before the request. LSP4J reads the messages on
 * another thread, which only queues them here, so it stays free for changes and cancellations while
 * a request is answered, and a request that is cancelled before its turn is skipped.
 */
@Slf4j
final class RequestThread implements AutoCloseable {

  private final ExecutorService executor =
      Executors.newSingleThreadExecutor(
          runnable -> {
            var thread = new Thread(runnable, "sqrl-lsp-requests");
            thread.setDaemon(true);
            return thread;
          });

  /** Handles a notification, e.g. a change of a document, after the messages before it. */
  void handle(Runnable notification) {
    executor.execute(
        () -> {
          try {
            notification.run();
          } catch (RuntimeException e) {
            log.error("Could not handle a notification of the editor", e);
          }
        });
  }

  /** Answers a request after the messages before it. */
  <T> CompletableFuture<T> answer(Supplier<T> answer) {
    return CompletableFuture.supplyAsync(answer, executor);
  }

  /**
   * Answers a request after the messages before it, with an answer that completes later, e.g. after
   * a check.
   */
  <T> CompletableFuture<T> answerLater(Supplier<CompletableFuture<T>> answer) {
    return answer(answer).thenCompose(later -> later);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
