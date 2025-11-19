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

import static com.datasqrl.function.CalciteFunctionUtil.lightweightAggOp;

import com.datasqrl.calcite.Dialect;
import lombok.Getter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.FunctionDefinition;

@Getter
public abstract class PostgresSqlTranslation extends AbstractDialectSqlTranslation {

  public PostgresSqlTranslation(SqlOperator operator) {
    super(Dialect.POSTGRES, operator);
  }

  public PostgresSqlTranslation(FunctionDefinition operator) {
    super(Dialect.POSTGRES, operator);
  }

  public static class Simple extends PostgresSqlTranslation {

    private final SqlOperator toOperator;

    public Simple(FunctionDefinition fromOperator, SqlOperator toOperator) {
      super(fromOperator);
      this.toOperator = toOperator;
    }

    public Simple(FunctionDefinition fromOperator, String toOperatorName) {
      this(fromOperator, lightweightAggOp(toOperatorName));
    }

    @Override
    public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
      toOperator
          .createCall(SqlParserPos.ZERO, call.getOperandList())
          .unparse(writer, leftPrec, rightPrec);
    }
  }
}
