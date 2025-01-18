package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import org.apache.calcite.sql.parser.SqlParserPos;

public class ParsePosUtil {

  public static FileLocation convertPosition(SqlParserPos parsePos) {
    return new FileLocation(parsePos.getLineNum(), parsePos.getColumnNum());
  }

}
