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
package com.datasqrl.function.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.flinkrunner.stdlib.json.JsonFunctions;
import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

// Disabled for now
// @AutoService(SqlTranslation.class)
public class JsonToStringTranslation extends SnowflakeSqlTranslation {

  public JsonToStringTranslation() {
    super(lightweightOp(JsonFunctions.JSON_TO_STRING));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightOp("TO_JSON")
        .createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
