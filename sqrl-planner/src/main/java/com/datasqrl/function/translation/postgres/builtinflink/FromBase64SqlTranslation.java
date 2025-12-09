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
public class FromBase64SqlTranslation extends PostgresSqlTranslation {

  public FromBase64SqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("FROM_BASE64"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var b64Lit = SqlLiteral.createCharString("base64", SqlParserPos.ZERO);
    var utf8Lit = SqlLiteral.createCharString("UTF8", SqlParserPos.ZERO);

    // convert_from(decode(<value>, 'base64'), 'UTF8')
    var convert = writer.startFunCall("CONVERT_FROM");
    var decode = writer.startFunCall("DECODE");
    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",", true);
    b64Lit.unparse(writer, 0, 0);
    writer.endFunCall(decode);
    writer.sep(",", true);
    utf8Lit.unparse(writer, 0, 0);
    writer.endFunCall(convert);
  }
}
