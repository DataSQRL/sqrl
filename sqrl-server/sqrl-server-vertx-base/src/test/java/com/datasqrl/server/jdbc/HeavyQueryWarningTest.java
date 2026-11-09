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
package com.datasqrl.server.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.config.HeavyQueryLogConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class HeavyQueryWarningTest {

  private static final String SQL = "SELECT *\n  FROM \"orders\"\n WHERE \"id\" = $1";

  private final HeavyQueryLogConfig config = new HeavyQueryLogConfig();

  @Test
  void givenSmallFastQuery_whenDescribe_thenNoWarning() {
    assertThat(HeavyQueryWarning.describe(config, "/orders", SQL, 9999, Duration.ofMillis(4999)))
        .isEmpty();
  }

  @Test
  void givenLargeResult_whenDescribe_thenWarnsWithPathSqlRowsAndTime() {
    assertThat(HeavyQueryWarning.describe(config, "/orders", SQL, 10000, Duration.ofMillis(120)))
        .contains(
            "Heavy query (large result) at /orders: 10000 rows in 120 ms;"
                + " sql: SELECT * FROM \"orders\" WHERE \"id\" = $1");
  }

  @Test
  void givenSlowQuery_whenDescribe_thenWarnsSlow() {
    assertThat(HeavyQueryWarning.describe(config, "/orders", SQL, 3, Duration.ofSeconds(5)))
        .contains(
            "Heavy query (slow query) at /orders: 3 rows in 5000 ms;"
                + " sql: SELECT * FROM \"orders\" WHERE \"id\" = $1");
  }

  @Test
  void givenLargeAndSlowQuery_whenDescribe_thenNamesBothReasons() {
    assertThat(
            HeavyQueryWarning.describe(
                config, "/customers[0]/orders", SQL, 28296, Duration.ofMillis(6200)))
        .contains(
            "Heavy query (large result, slow query) at /customers[0]/orders: 28296 rows in 6200"
                + " ms; sql: SELECT * FROM \"orders\" WHERE \"id\" = $1");
  }

  @Test
  void givenDisabledThresholds_whenDescribe_thenNoWarning() {
    config.setRowCountThreshold(0);
    config.setElapsedThresholdMs(-1);

    assertThat(HeavyQueryWarning.describe(config, "/orders", SQL, 1_000_000, Duration.ofMinutes(1)))
        .isEmpty();
  }
}
