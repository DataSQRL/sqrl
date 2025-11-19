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
package com.datasqrl.function.translation.postgres.builtinflink;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.translation.PostgresSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class Sha1SqlTranslation extends PostgresSqlTranslation {

  public Sha1SqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("SHA1"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var string = call.getOperandList().get(0);
    var sha1Literal = SqlLiteral.createCharString("sha1", SqlParserPos.ZERO);
    var hexLiteral = SqlLiteral.createCharString("hex", SqlParserPos.ZERO);

    var digestCall =
        CalciteFunctionUtil.lightweightOp("digest")
            .createCall(SqlParserPos.ZERO, string, sha1Literal);

    CalciteFunctionUtil.lightweightOp("encode")
        .createCall(SqlParserPos.ZERO, digestCall, hexLiteral)
        .unparse(writer, leftPrec, rightPrec);
  }
}
