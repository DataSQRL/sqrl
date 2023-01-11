package com.datasqrl.error;

import com.datasqrl.error.ErrorMessage.Implementation;
import com.datasqrl.error.ErrorMessage.Severity;
import com.google.auto.service.AutoService;

@AutoService(ErrorHandler.class)
public class LocationExceptionHandler implements ErrorHandler<LocationException> {

  @Override
  public ErrorMessage handle(LocationException e, ErrorLocation baseLocation) {
    //Ignores baseLocation since exception has location
    return new Implementation(e.getLabel(), e.getMessage(), e.getLocation(), Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return LocationException.class;
  }

}
