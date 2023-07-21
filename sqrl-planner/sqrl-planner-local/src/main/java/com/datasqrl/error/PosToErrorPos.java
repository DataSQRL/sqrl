package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileRange;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PosToErrorPos {

  public static ErrorLocation atPosition(ErrorLocation location, SqlParserPos pos) {
    return location.atFile(new FileRange(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()));
  }

  public static ErrorLocation atPosition(ErrorCollector errors, SqlParserPos pos) {
    return errors.getLocation().atFile(new FileRange(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()));
  }
}
