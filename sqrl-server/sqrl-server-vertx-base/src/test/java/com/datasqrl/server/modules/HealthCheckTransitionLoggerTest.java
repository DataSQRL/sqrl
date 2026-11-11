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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.modules.HealthCheckTransitionLogger.Transition;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.CheckResult;
import io.vertx.ext.healthchecks.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

class HealthCheckTransitionLoggerTest {

  private final HealthCheckTransitionLogger logger = new HealthCheckTransitionLogger();

  @Test
  void givenFirstProbeUp_whenObserve_thenUnchanged() {
    assertThat(logger.observe("kafka", true)).isEqualTo(Transition.UNCHANGED);
  }

  @Test
  void givenFirstProbeKo_whenObserve_thenWentKo() {
    assertThat(logger.observe("kafka", false)).isEqualTo(Transition.WENT_KO);
  }

  @Test
  void givenUpThenKo_whenObserve_thenWentKoOnlyOnce() {
    logger.observe("kafka", true);

    assertThat(logger.observe("kafka", false)).isEqualTo(Transition.WENT_KO);
    assertThat(logger.observe("kafka", false)).isEqualTo(Transition.UNCHANGED);
  }

  @Test
  void givenKo_whenObserveUp_thenRecoveredOnlyOnce() {
    logger.observe("kafka", false);

    assertThat(logger.observe("kafka", true)).isEqualTo(Transition.RECOVERED);
    assertThat(logger.observe("kafka", true)).isEqualTo(Transition.UNCHANGED);
  }

  @Test
  void givenTwoChecks_whenOneGoesKo_thenOtherIsTrackedSeparately() {
    logger.observe("kafka", false);

    assertThat(logger.observe("database", false)).isEqualTo(Transition.WENT_KO);
    assertThat(logger.observe("kafka", true)).isEqualTo(Transition.RECOVERED);
  }

  @Test
  void givenAggregateResult_whenApply_thenTracksEachCheckAndReturnsResultUnchanged() {
    var kafka = CheckResult.from("kafka", Status.KO(new JsonObject().put("reason", "broker down")));
    var result = new CheckResult().setChecks(List.of(kafka));

    assertThat(logger.apply(result).result()).isSameAs(result);
    assertThat(logger.observe("kafka", false)).isEqualTo(Transition.UNCHANGED);
  }
}
