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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

abstract class BaseShaSqlTranslation extends PostgresSqlTranslation {

  private final String shaAlg;

  BaseShaSqlTranslation(String shaAlg) {
    super(CalciteFunctionUtil.lightweightOp(shaAlg.toUpperCase()));
    this.shaAlg = shaAlg;
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var shaLit = SqlLiteral.createCharString(shaAlg, SqlParserPos.ZERO);
    var hexLit = SqlLiteral.createCharString("hex", SqlParserPos.ZERO);

    // encode(digest(<value>, 'sha224'), 'hex')
    var encode = writer.startFunCall("ENCODE");
    var digest = writer.startFunCall("DIGEST");
    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",", true);
    shaLit.unparse(writer, 0, 0);
    writer.endFunCall(digest);
    writer.sep(",", true);
    hexLit.unparse(writer, 0, 0);
    writer.endFunCall(encode);
  }
}
