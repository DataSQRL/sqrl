package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileRange;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PosToErrorPos {

  public static ErrorLocation atPosition(ErrorCollector errors, SqlParserPos pos) {
    return errors.getLocation().atFile(new FileRange(pos.getLineNum(), pos.getColumnNum() + 1, pos.getEndLineNum(), pos.getEndColumnNum() + 1));
  }
}
