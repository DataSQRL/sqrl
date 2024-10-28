package com.datasqrl.function.translations;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

@AutoService(SqlTranslation.class)
public class NowSqlTranslation extends PostgresSqlTranslation {

  public NowSqlTranslation() {
    super(lightweightOp("now", ReturnTypes.explicit(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3)));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlStdOperatorTable.CURRENT_TIMESTAMP.createCall(SqlParserPos.ZERO)
        .unparse(writer, leftPrec, rightPrec);
  }
}
