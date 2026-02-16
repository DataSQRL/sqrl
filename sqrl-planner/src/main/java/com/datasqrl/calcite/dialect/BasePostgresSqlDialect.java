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
package com.datasqrl.calcite.dialect;

import static org.apache.calcite.sql.SqlKind.COLLECTION_TABLE;

import com.datasqrl.function.translation.SqlTranslation;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

abstract class BasePostgresSqlDialect extends PostgresqlSqlDialect {

  public BasePostgresSqlDialect(Context context) {
    super(context);
  }

  protected abstract Map<String, SqlTranslation> getTranslationMap();

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getOperator().getKind() == COLLECTION_TABLE) { // skip FROM TABLE(..) call
      unparseCall(writer, (SqlCall) call.getOperandList().get(0), leftPrec, rightPrec);
      return;
    }

    var operatorName = call.getOperator().getName().toLowerCase();
    if (getTranslationMap().containsKey(operatorName)) {
      getTranslationMap().get(operatorName).unparse(call, writer, leftPrec, rightPrec);
      return;
    }
    try {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException(
          "Could not unparse:"
              + call.getOperator().getName()
              + " -> "
              + call.getOperandList().toString(),
          e);
    }
  }
}
