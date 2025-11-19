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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class Sha2SqlTranslation extends PostgresSqlTranslation {

  public Sha2SqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("SHA2"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var string = call.getOperandList().get(0);
    var hashLength = call.getOperandList().get(1);
    var hexLiteral = SqlLiteral.createCharString("hex", SqlParserPos.ZERO);

    var sha224Literal = SqlLiteral.createCharString("sha224", SqlParserPos.ZERO);
    var sha256Literal = SqlLiteral.createCharString("sha256", SqlParserPos.ZERO);
    var sha384Literal = SqlLiteral.createCharString("sha384", SqlParserPos.ZERO);
    var sha512Literal = SqlLiteral.createCharString("sha512", SqlParserPos.ZERO);

    var literal224 = SqlLiteral.createExactNumeric("224", SqlParserPos.ZERO);
    var literal256 = SqlLiteral.createExactNumeric("256", SqlParserPos.ZERO);
    var literal384 = SqlLiteral.createExactNumeric("384", SqlParserPos.ZERO);
    var literal512 = SqlLiteral.createExactNumeric("512", SqlParserPos.ZERO);

    var digest224 =
        CalciteFunctionUtil.lightweightOp("digest")
            .createCall(SqlParserPos.ZERO, string, sha224Literal);
    var encode224 =
        CalciteFunctionUtil.lightweightOp("encode")
            .createCall(SqlParserPos.ZERO, digest224, hexLiteral);

    var digest256 =
        CalciteFunctionUtil.lightweightOp("digest")
            .createCall(SqlParserPos.ZERO, string, sha256Literal);
    var encode256 =
        CalciteFunctionUtil.lightweightOp("encode")
            .createCall(SqlParserPos.ZERO, digest256, hexLiteral);

    var digest384 =
        CalciteFunctionUtil.lightweightOp("digest")
            .createCall(SqlParserPos.ZERO, string, sha384Literal);
    var encode384 =
        CalciteFunctionUtil.lightweightOp("encode")
            .createCall(SqlParserPos.ZERO, digest384, hexLiteral);

    var digest512 =
        CalciteFunctionUtil.lightweightOp("digest")
            .createCall(SqlParserPos.ZERO, string, sha512Literal);
    var encode512 =
        CalciteFunctionUtil.lightweightOp("encode")
            .createCall(SqlParserPos.ZERO, digest512, hexLiteral);

    var when224 = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, hashLength, literal224);
    var when256 = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, hashLength, literal256);
    var when384 = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, hashLength, literal384);
    var when512 = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, hashLength, literal512);

    SqlStdOperatorTable.CASE
        .createCall(
            SqlParserPos.ZERO,
            when224,
            encode224,
            when256,
            encode256,
            when384,
            encode384,
            when512,
            encode512,
            encode256)
        .unparse(writer, leftPrec, rightPrec);
  }
}
