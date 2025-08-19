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
import org.apache.flink.util.TimeUtils;

public class TtlHint extends PlannerHint {

  public static final String HINT_NAME = "ttl";

  private final Duration ttl;

  protected TtlHint(ParsedObject<SqrlHint> source, Duration ttlDuration) {
    super(source, Type.DAG);
    this.ttl = ttlDuration;
  }

  public Optional<Duration> getTtl() {
    return Optional.ofNullable(ttl);
  }

  @AutoService(Factory.class)
  public static class TtlHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new TtlHint(source, parseDuration(source));
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

  public static Duration parseDuration(ParsedObject<SqrlHint> source) {
    var arguments = source.get().getOptions();
    if (arguments == null || arguments.isEmpty()) {
      return null;
    }
    if (arguments.size() != 1 || arguments.get(0) == null) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "%s hint only supports one duration argument (e.g. `2 days`).",
          source.get().getName());
    }
    try {
      return TimeUtils.parseDuration(arguments.get(0));
    } catch (Exception e) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "%s hint does not have a valid duration argument: %s. Expected `2 days` or `10 s`. "
              + e.getMessage(),
          source.get().getName(),
          arguments.get(0));
    }
  }
}
