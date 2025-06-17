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

import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.google.auto.service.AutoService;

/** Assigns a partition key to a table that is persisted into a data system engine. */
public class PrimaryKeyHint extends ColumnNamesHint {

  public static final String HINT_NAME = "primary_key";

  protected PrimaryKeyHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER, source.get().getOptions());
  }

  @AutoService(Factory.class)
  public static class PKFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new PrimaryKeyHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
