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
import com.datasqrl.plan.global.IndexType;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.util.EnumUtil;
import com.google.auto.service.AutoService;
import java.util.List;
import lombok.Getter;

/**
 * Explicitly assign an index to a table that's persisted to a database engine. Overwrites the
 * automatically determined index structures.
 */
@Getter
public class IndexHint extends ColumnNamesHint {

  public static final String HINT_NAME = "index";

  private final IndexType indexType;

  protected IndexHint(
      ParsedObject<SqrlHint> source, IndexType indexType, List<String> columnsNames) {
    super(source, Type.DAG, columnsNames);
    this.indexType = indexType;
  }

  @AutoService(Factory.class)
  public static class IndexHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var arguments = source.get().getOptions();
      if (arguments == null || arguments.isEmpty()) {
        return new IndexHint(source, null, List.of()); // no hint
      }
      if (arguments.size() <= 1) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "Index hint requires at least two arguments: the name of the index type and at least one column.");
      }
      var optIndex = EnumUtil.getByName(IndexType.class, arguments.get(0));
      if (optIndex.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "Unknown index type: %s",
            arguments.get(0));
      }
      return new IndexHint(source, optIndex.get(), arguments.subList(1, arguments.size()));
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
