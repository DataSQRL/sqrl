/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorMessage.Implementation;
import com.datasqrl.error.ErrorMessage.Severity;
import com.datasqrl.parse.SqrlAstException;

public class SqrlAstExceptionHandler implements ErrorHandler<SqrlAstException> {

  @Override
  public ErrorMessage handle(SqrlAstException e, ErrorLocation baseLocation) {

    return new Implementation(e.getErrorLabel(), e.getMessage(),
        e.getPos() != null ? baseLocation.atFile(e.getLocation()) :
        baseLocation, Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return SqrlAstException.class;
  }
}
