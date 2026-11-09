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

import com.datasqrl.server.config.HeavyQueryLogConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import lombok.experimental.UtilityClass;

@UtilityClass
class HeavyQueryWarning {

  Optional<String> describe(
      HeavyQueryLogConfig config, String fieldPath, String sql, int rowCount, Duration elapsed) {
    var reasons = new ArrayList<String>();
    if (reaches(rowCount, config.getRowCountThreshold())) {
      reasons.add("large result");
    }
    if (reaches(elapsed.toMillis(), config.getElapsedThresholdMs())) {
      reasons.add("slow query");
    }
    if (reasons.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        "Heavy query (%s) at %s: %d rows in %d ms; sql: %s"
            .formatted(
                String.join(", ", reasons), fieldPath, rowCount, elapsed.toMillis(), oneLine(sql)));
  }

  String oneLine(String sql) {
    return sql.strip().replaceAll("\\s+", " ");
  }

  private boolean reaches(long value, long threshold) {
    return threshold > 0 && value >= threshold;
  }
}
