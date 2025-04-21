package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightBiOp;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.types.json.functions.JsonFunctions;
import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class JsonToStringTranslation extends PostgresSqlTranslation {

  public JsonToStringTranslation() {
    super(lightweightOp(JsonFunctions.JSON_TO_STRING));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightBiOp("#>>").createCall(SqlParserPos.ZERO,
            List.of(call.getOperandList().get(0), SqlLiteral.createCharString("{}", SqlParserPos.ZERO)))
        .unparse(writer, leftPrec, rightPrec);
  }
}
