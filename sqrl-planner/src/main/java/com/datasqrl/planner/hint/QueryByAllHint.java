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

/**
 * Defines a table access function for the table that queries by all of the given columns (i.e. an
 * AND condition) and all columns must be set (i.e. no nulls accepted)
 */
public class QueryByAllHint extends ColumnNamesHint implements QueryByHint {

  public static final String HINT_NAME = "query_by_all";

  protected QueryByAllHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER, source.get().getOptions());
  }

  @AutoService(Factory.class)
  public static class QueryByAllFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      return new QueryByAllHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
