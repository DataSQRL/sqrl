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
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.ServiceLoaderException;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Getter;

/** Abstract hint class that provides basic methods for interacting with hints */
@Getter
public abstract class PlannerHint implements Hint {

  protected final ParsedObject<SqrlHint> source;
  protected final Type type;

  protected PlannerHint(ParsedObject<SqrlHint> source, Type type) {
    Preconditions.checkArgument(source.isPresent());
    this.source = source;
    this.type = type;
  }

  @Override
  public List<String> getOptions() {
    return source.get().getOptions();
  }

  @Override
  public String getName() {
    return source.get().getName().toLowerCase();
  }

  public enum Type {
    ANALYZER,
    DAG
  }

  public static PlannerHint from(ParsedObject<SqrlHint> sqrlHint) {
    Preconditions.checkArgument(sqrlHint.isPresent());
    try {
      var factory =
          ServiceLoaderDiscovery.get(Factory.class, Factory::getName, sqrlHint.get().getName());
      return factory.create(sqrlHint);
    } catch (ServiceLoaderException e) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          sqrlHint.getFileLocation(),
          "Unrecognized hint [%s]",
          sqrlHint.get().getName());
    }
  }

  public interface Factory {
    PlannerHint create(ParsedObject<SqrlHint> source);

    String getName();
  }
}
