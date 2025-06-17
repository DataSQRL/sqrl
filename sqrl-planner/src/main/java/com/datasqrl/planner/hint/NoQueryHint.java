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
import com.google.common.base.Preconditions;
import java.util.List;

public class NoQueryHint extends ColumnNamesHint implements QueryByHint {

  public static final String HINT_NAME = "no_query";

  protected NoQueryHint(ParsedObject<SqrlHint> source) {
    super(source, Type.ANALYZER, List.of());
  }

  @AutoService(Factory.class)
  public static class NoQueryHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      Preconditions.checkArgument(
          source.get().getOptions().isEmpty(), "no_query hint does not accept options");
      return new NoQueryHint(source);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
