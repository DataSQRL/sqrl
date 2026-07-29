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
package com.datasqrl.planner.hint;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.util.TimeUtils;
import com.google.auto.service.AutoService;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class TtlHint extends PlannerHint {

  public static final String HINT_NAME = "ttl";

  private final Duration ttl;
  private final ChronoUnit ttlUnit;

  protected TtlHint(ParsedObject<SqrlHint> source, Duration ttl, ChronoUnit ttlUnit) {
    super(source, Type.DAG);
    this.ttl = ttl;
    this.ttlUnit = ttlUnit;
  }

  public Optional<Duration> getTtl() {
    return Optional.ofNullable(ttl);
  }

  /** The unit the TTL was declared with - determines the smallest allowed partition width */
  public Optional<ChronoUnit> getTtlUnit() {
    return Optional.ofNullable(ttlUnit);
  }

  @AutoService(Factory.class)
  public static class TtlHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var arguments = source.get().options();
      if (arguments == null || arguments.isEmpty()) {
        return new TtlHint(source, null, null);
      }
      if (arguments.size() != 1 || arguments.get(0) == null) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "%s hint only supports one duration argument (e.g. `2 days`).",
            source.get().name());
      }
      return parseTtlArgument(source, arguments.get(0).trim());
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

  private static TtlHint parseTtlArgument(ParsedObject<SqrlHint> source, String argument) {
    Duration ttl = null;
    ChronoUnit unit = null;
    try {
      ttl = TimeUtils.parseDuration(argument);
      unit = TimeUtils.parseDurationUnit(argument);
    } catch (Exception e) {
      // fall through to the shared error below
    }
    if (unit == null
        || unit.compareTo(ChronoUnit.MINUTES) < 0
        || unit.compareTo(ChronoUnit.DAYS) > 0) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "%s hint does not have a valid duration argument: %s. Expected a positive number with a"
              + " unit between minute and day, e.g. `30 min`, `36 hours`, or `14 days`.",
          source.get().name(),
          argument);
    }
    return new TtlHint(source, ttl, unit);
  }

  public static Duration parseDuration(ParsedObject<SqrlHint> source) {
    var arguments = source.get().options();
    if (arguments == null || arguments.isEmpty()) {
      return null;
    }
    if (arguments.size() != 1 || arguments.get(0) == null) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "%s hint only supports one duration argument (e.g. `2 days`).",
          source.get().name());
    }
    return parseDurationArgument(source, arguments.get(0));
  }

  private static Duration parseDurationArgument(ParsedObject<SqrlHint> source, String argument) {
    try {
      return TimeUtils.parseDuration(argument);
    } catch (Exception e) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "%s hint does not have a valid duration argument: %s. Expected `2 days` or `10 s`. "
              + e.getMessage(),
          source.get().name(),
          argument);
    }
  }
}
