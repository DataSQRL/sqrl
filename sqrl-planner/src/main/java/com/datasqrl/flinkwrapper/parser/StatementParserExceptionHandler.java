package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorHandler;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorMessage;
import com.google.auto.service.AutoService;

@AutoService(ErrorHandler.class)
public class StatementParserExceptionHandler implements ErrorHandler<StatementParserException> {

  @Override
  public ErrorMessage handle(StatementParserException e, ErrorLocation baseLocation) {
    return new ErrorMessage.Implementation(e.errorLabel, e.getMessage(),
        baseLocation.atFile(baseLocation.getFileLocation().add(e.fileLocation)),
        ErrorMessage.Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return StatementParserException.class;
  }
}