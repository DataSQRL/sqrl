/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorMessage.Implementation;
import com.datasqrl.error.ErrorMessage.Severity;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import lombok.Getter;

public class ErrorCatcher implements Serializable {

  protected static final Map<Class, ErrorHandler> handlers = loadHandlers();

  @Getter private final ErrorLocation baseLocation;
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
    return handle(e, null);
  }

  public CollectedException handle(Throwable e, String messagePrefix) {
    if (e instanceof CollectedException exception) {
      return exception; // has already been handled
    }
    Optional<ErrorHandler> handler = Optional.ofNullable(handlers.get(e.getClass()));
    ErrorMessage msg;
    if (handler.isPresent()) {
      msg = handler.get().handle((Exception) e, baseLocation, messagePrefix);
    } else {
      msg =
          new Implementation(
              ErrorLabel.GENERIC, messagePrefix, e.getMessage(), baseLocation, Severity.FATAL);
    }
    errors.addInternal(msg);
    return new CollectedException(e);
  }
}
