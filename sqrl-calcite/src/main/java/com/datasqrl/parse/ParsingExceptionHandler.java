/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.parse;

import com.datasqrl.error.ErrorHandler;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.error.ErrorMessage.Implementation;
import com.datasqrl.error.ErrorMessage.Severity;
import com.google.auto.service.AutoService;

@AutoService(ErrorHandler.class)
public class ParsingExceptionHandler implements ErrorHandler<ParsingException> {

  @Override
  public ErrorMessage handle(ParsingException e, ErrorLocation baseLocation) {
    return new Implementation(ErrorLabel.GENERIC, e.getErrorMessage(),
        baseLocation.atFile(new FileLocation(e.getLineNumber(), e.getColumnNumber()))
        , Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return ParsingException.class;
  }
}
