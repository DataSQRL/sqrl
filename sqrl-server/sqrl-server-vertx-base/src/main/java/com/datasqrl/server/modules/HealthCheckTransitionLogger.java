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
package com.datasqrl.server.modules;

import io.vertx.core.Future;
import io.vertx.ext.healthchecks.CheckResult;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/** Logs each health check once when it goes KO and once when it recovers, not on every probe. */
@Slf4j
class HealthCheckTransitionLogger implements Function<CheckResult, Future<CheckResult>> {

  enum Transition {
    WENT_KO,
    RECOVERED,
    UNCHANGED
  }

  private final Map<String, Boolean> lastUp = new ConcurrentHashMap<>();

  @Override
  public Future<CheckResult> apply(CheckResult result) {
    logTransitions(result);
    return Future.succeededFuture(result);
  }

  private void logTransitions(CheckResult result) {
    var checks = result.getChecks();
    if (checks != null && !checks.isEmpty()) {
      checks.forEach(this::logTransitions);
      return;
    }
    if (result.getId() == null) {
      return;
    }
    switch (observe(result.getId(), CheckResult.isUp(result))) {
      case WENT_KO ->
          log.warn(
              "Health check '{}' is KO: {}", result.getId(), result.getData(), result.getFailure());
      case RECOVERED -> log.info("Health check '{}' recovered", result.getId());
      case UNCHANGED -> {}
    }
  }

  Transition observe(String checkId, boolean up) {
    var wasUp = lastUp.put(checkId, up);
    if (!up && !Boolean.FALSE.equals(wasUp)) {
      return Transition.WENT_KO;
    }
    if (up && Boolean.FALSE.equals(wasUp)) {
      return Transition.RECOVERED;
    }
    return Transition.UNCHANGED;
  }
}
