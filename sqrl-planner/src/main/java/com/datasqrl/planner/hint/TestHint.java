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
import lombok.Getter;

/**
 * Defines a table to be a test case which means it will only be planned when the user runs test
 * cases
 */
public class TestHint extends PlannerHint {

  public static final String HINT_NAME = "test";

  @Getter private final boolean noRows;

  protected TestHint(ParsedObject<SqrlHint> source, boolean noRows) {
    super(source, Type.DAG);
    this.noRows = noRows;
  }

  @AutoService(Factory.class)
  public static class TestHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new TestHint(source, parseNoRows(source));
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }

    private static boolean parseNoRows(ParsedObject<SqrlHint> source) {
      var arguments = source.get().getOptions();
      if (arguments == null || arguments.isEmpty()) {
        return false;
      }

      if (arguments.size() != 1 || !"no_rows".equalsIgnoreCase(arguments.get(0))) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "%s hint only supports 'no_rows' as argument",
            source.get().getName());
      }

      return true;
    }
  }
}
