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
package com.datasqrl.util;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Duration parsing utilities built on top of Flink's {@link org.apache.flink.util.TimeUtils}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TimeUtils {

  /**
   * Parses the given string to a {@link Duration}, accepting the same formats as Flink's {@link
   * org.apache.flink.util.TimeUtils#parseDuration(String)}, e.g. {@code 30 min}, {@code 2 days}. If
   * no unit label is specified, the value is interpreted as milliseconds.
   */
  public static Duration parseDuration(String text) {
    return org.apache.flink.util.TimeUtils.parseDuration(text);
  }

  /**
   * Extracts the unit the given duration string was declared with, e.g. {@code 2 days} yields
   * {@link ChronoUnit#DAYS}. If no unit label is specified, {@link ChronoUnit#MILLIS} is returned,
   * mirroring {@link #parseDuration(String)}.
   */
  public static ChronoUnit parseDurationUnit(String text) {
    var trimmed = text.trim();
    var pos = 0;
    while (pos < trimmed.length() && Character.isDigit(trimmed.charAt(pos))) {
      pos++;
    }
    if (pos == 0) {
      throw new NumberFormatException("text does not start with a number");
    }
    // Flink keeps its label-to-unit map private, so recover the unit by parsing a unit-sized
    // duration through Flink and matching it back to a ChronoUnit
    var unitDuration = org.apache.flink.util.TimeUtils.parseDuration("1" + trimmed.substring(pos));
    return Arrays.stream(ChronoUnit.values())
        .filter(unit -> unit.getDuration().equals(unitDuration))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unrecognized time unit in: " + trimmed));
  }
}
