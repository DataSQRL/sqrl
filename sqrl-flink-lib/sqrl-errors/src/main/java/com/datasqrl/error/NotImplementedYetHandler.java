package com.datasqrl.error;

public class NotImplementedYetHandler implements ErrorHandler<NotYetImplementedException> {

  @Override
  public ErrorMessage handle(NotYetImplementedException e, ErrorLocation baseLocation) {
    return new ErrorMessage.Implementation(ErrorCode.NOT_YET_IMPLEMENTED, e.getMessage(),
        baseLocation, ErrorMessage.Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return NotYetImplementedException.class;
  }
}
