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
package com.datasqrl.engine.database.relational;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Computes the pg_partman partition width for a range-partitioned table with a TTL. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PostgresPartitionInterval {

  /** Calendar-aligned partition widths pg_partman can use, keyed by width in minutes. */
  private static final Map<Integer, String> PARTITION_MENU =
      ImmutableMap.<Integer, String>builder()
          .put(15, "15 minutes")
          .put(30, "30 minutes")
          .put(60, "1 hour")
          .put(120, "2 hours")
          .put(240, "4 hours")
          .put(360, "6 hours")
          .put(480, "8 hours")
          .put(720, "12 hours")
          .put(1440, "1 day")
          .put(2880, "2 days")
          .put(5760, "4 days")
          .put(10080, "1 week")
          .put(20160, "2 weeks")
          .put(40320, "4 weeks")
          .put(80640, "8 weeks")
          .put(120960, "12 weeks")
          .buildOrThrow();

  /**
   * Picks the partition width for the given TTL: the TTL divided by the divisor caps the partition
   * count, while the unit the TTL was declared with sets the floor. The result is snapped down to
   * the closest calendar-aligned width from the menu.
   */
  public static String asString(Duration ttl, ChronoUnit ttlUnit, int partitionTtlDivisor) {
    var floorMinutes = ttlUnit.getDuration().toMinutes();
    var targetMinutes = Math.max(ttl.toMinutes() / (double) partitionTtlDivisor, floorMinutes);

    String interval = null;
    for (var entry : PARTITION_MENU.entrySet()) {
      if (interval == null || entry.getKey() <= targetMinutes) {
        interval = entry.getValue();
      }
    }

    return interval;
  }
}
