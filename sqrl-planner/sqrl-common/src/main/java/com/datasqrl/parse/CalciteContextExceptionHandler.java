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
import org.apache.calcite.runtime.CalciteContextException;

@AutoService(ErrorHandler.class)
public class CalciteContextExceptionHandler implements ErrorHandler<CalciteContextException> {

  @Override
  public ErrorMessage handle(CalciteContextException e, ErrorLocation baseLocation) {
    return new Implementation(ErrorLabel.GENERIC, e.getMessage(),
        baseLocation.atFile(
            new FileLocation(e.getPosLine(), e.getPosColumn())),
        Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return CalciteContextException.class;
  }
}
