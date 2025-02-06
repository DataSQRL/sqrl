package com.datasqrl.error;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

import com.datasqrl.error.ErrorMessage.Implementation;
import com.datasqrl.error.ErrorMessage.Severity;

import lombok.Getter;

public class ErrorCatcher implements Serializable {

  protected static final Map<Class, ErrorHandler> handlers = loadHandlers();

  @Getter
  private final ErrorLocation baseLocation;
  private final ErrorCollection errors;

  public ErrorCatcher(ErrorLocation baseLocation, ErrorCollection errors) {
    this.baseLocation = baseLocation;
    this.errors = errors;
  }

  private static Map<Class, ErrorHandler> loadHandlers() {
    Map<Class, ErrorHandler> handlers = new HashMap<>();
    ServiceLoader<ErrorHandler> serviceLoader = ServiceLoader.load(ErrorHandler.class);
    for (ErrorHandler handler : serviceLoader) {
      handlers.put(handler.getHandleClass(), handler);
    }
    return handlers;
  }

  public CollectedException handle(Throwable e) {
    if (e instanceof CollectedException exception)
	 {
		return exception; //has already been handled
	}
    Optional<ErrorHandler> handler = Optional.ofNullable(handlers.get(e.getClass()));
    ErrorMessage msg;
    if (handler.isPresent()) {
      msg = handler.get().handle((Exception)e, baseLocation);
    } else {
      msg = new Implementation(ErrorLabel.GENERIC, e.getMessage(), baseLocation, Severity.FATAL);
    }
    errors.addInternal(msg);
    return new CollectedException(e);
  }


}
