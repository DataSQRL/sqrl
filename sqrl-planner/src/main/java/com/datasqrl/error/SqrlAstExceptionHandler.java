/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorMessage.Implementation;
import com.datasqrl.error.ErrorMessage.Severity;
import com.datasqrl.parse.SqrlAstException;
import java.util.Optional;

public class SqrlAstExceptionHandler implements ErrorHandler<SqrlAstException> {

  @Override
  public ErrorMessage handle(SqrlAstException e, ErrorEmitter emitter) {
    return new Implementation(Optional.ofNullable(e.getErrorCode()), e.getMessage(),
        emitter.getBaseLocation()
            .atFile(new FileLocation(e.getPos().getLineNum(), e.getPos().getColumnNum())),
        Severity.FATAL,
        emitter.getSourceMap());
  }

  @Override
  public Class getHandleClass() {
    return SqrlAstException.class;
  }
}
