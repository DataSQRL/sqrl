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
package com.datasqrl.function.translation;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.CalciteFunctionUtil;
import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;

@Getter
public abstract class AbstractDialectSqlTranslation implements SqlTranslation {

  private final Dialect dialect;
  private final SqlOperator operator;

  public AbstractDialectSqlTranslation(Dialect dialect, SqlOperator operator) {
    this.dialect = dialect;
    this.operator = operator;
  }

  public AbstractDialectSqlTranslation(Dialect dialect, FunctionDefinition operator) {
    this(dialect, CalciteFunctionUtil.lightweightOp(operator));
  }
}
