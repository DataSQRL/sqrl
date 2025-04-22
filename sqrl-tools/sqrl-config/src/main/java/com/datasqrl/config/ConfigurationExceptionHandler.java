package com.datasqrl.config;

import org.apache.commons.configuration2.ex.ConfigurationException;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorHandler;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorMessage;
import com.google.auto.service.AutoService;

@AutoService(ErrorHandler.class)
public class ConfigurationExceptionHandler implements ErrorHandler<ConfigurationException> {

  @Override
  public ErrorMessage handle(ConfigurationException e, ErrorLocation baseLocation) {
    var message = e.getMessage();
    if (e.getCause()!=null && e.getCause()!=e) {
      message += ": " + e.getCause().getMessage();
    }
    return new ErrorMessage.Implementation(ErrorCode.CONFIG_EXCEPTION, message, baseLocation, ErrorMessage.Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return ConfigurationException.class;
  }
}