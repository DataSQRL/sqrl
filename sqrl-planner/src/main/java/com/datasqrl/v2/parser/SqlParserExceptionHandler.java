package com.datasqrl.v2.parser;

import static com.datasqrl.v2.parser.ParsePosUtil.convertPosition;

import com.datasqrl.error.ErrorHandler;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorMessage;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.table.api.SqlParserException;

@AutoService(ErrorHandler.class)
public class SqlParserExceptionHandler implements ErrorHandler<SqlParserException> {

  @Override
  public ErrorMessage handle(SqlParserException e, ErrorLocation baseLocation) {
    FileLocation location;
    if (e.getCause() instanceof SqlParseException) {
      location = convertPosition(((SqlParseException) e.getCause()).getPos());
    } else {
      location = new FileLocation(1,1);
    }
    return new ErrorMessage.Implementation(ErrorLabel.GENERIC, e.getMessage(),
        baseLocation.atFile(location),ErrorMessage.Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return SqlParserException.class;
  }
}