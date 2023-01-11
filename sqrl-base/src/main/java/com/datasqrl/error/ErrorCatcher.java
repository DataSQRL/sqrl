package com.datasqrl.error;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
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

  public void handle(Exception e) {
    Optional<ErrorHandler> handler = Optional.ofNullable(handlers.get(e.getClass()));
    handler.map(h -> h.handle(e, baseLocation)).ifPresent(errors::addInternal);
  }


}
