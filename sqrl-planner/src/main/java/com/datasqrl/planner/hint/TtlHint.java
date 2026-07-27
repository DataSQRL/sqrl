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
import com.google.auto.service.AutoService;
import java.time.Duration;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.flink.util.TimeUtils;

public class TtlHint extends PlannerHint {

  public static final String HINT_NAME = "ttl";

  /** Subset of postgres interval syntax accepted as an explicit partition interval */
  private static final Pattern PARTITION_INTERVAL_PATTERN =
      Pattern.compile(
          "^\\d+\\s*(second|minute|hour|day|week|month|year)s?$", Pattern.CASE_INSENSITIVE);

  private final Duration ttl;
  private final String partitionInterval;

  protected TtlHint(ParsedObject<SqrlHint> source, Duration ttlDuration, String partitionInterval) {
    super(source, Type.DAG);
    this.ttl = ttlDuration;
    this.partitionInterval = partitionInterval;
  }

  public Optional<Duration> getTtl() {
    return Optional.ofNullable(ttl);
  }

  public Optional<String> getPartitionInterval() {
    return Optional.ofNullable(partitionInterval);
  }

  @AutoService(Factory.class)
  public static class TtlHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var arguments = source.get().options();
      if (arguments == null || arguments.isEmpty()) {
        return new TtlHint(source, null, null);
      }
      if (arguments.size() > 2 || arguments.get(0) == null) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "%s hint supports a duration argument and an optional partition interval argument"
                + " (e.g. `14 days, 1 day`).",
            source.get().name());
      }
      var ttl = parseDurationArgument(source, arguments.get(0));
      String partitionInterval = null;
      if (arguments.size() == 2) {
        partitionInterval = parsePartitionInterval(source, arguments.get(1));
      }
      return new TtlHint(source, ttl, partitionInterval);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
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

  private static String parsePartitionInterval(ParsedObject<SqrlHint> source, String argument) {
    if (argument == null || !PARTITION_INTERVAL_PATTERN.matcher(argument).matches()) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "%s hint does not have a valid partition interval argument: %s. Expected an interval"
              + " like `1 day` or `1 month`.",
          source.get().name(),
          argument);
    }
    return argument;
  }
}
