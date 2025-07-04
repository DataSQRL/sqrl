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
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.util.EnumUtil;
import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Optional;
import lombok.Getter;

/**
 * Execution hints allow the user to assign a stage to a table/function definition and the
 * corresponding node in the DAG.
 */
public class MutationInsertHint extends PlannerHint {

  public static final String HINT_NAME = "insert";

  @Getter private final MutationInsertType insertType;

  protected MutationInsertHint(ParsedObject<SqrlHint> source, MutationInsertType insertType) {
    super(source, Type.DAG);
    this.insertType = insertType;
  }

  @AutoService(Factory.class)
  public static class MutationInsertHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var arguments = source.get().getOptions();
      final Optional<MutationInsertType> insertType;
      if (arguments == null
          || arguments.size() != 1
          || (insertType = EnumUtil.getByName(MutationInsertType.class, arguments.get(0)))
              .isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "Insert hint requires exactly one argument from: "
                + Arrays.toString(MutationInsertType.values()));
      }
      return new MutationInsertHint(source, insertType.get());
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
