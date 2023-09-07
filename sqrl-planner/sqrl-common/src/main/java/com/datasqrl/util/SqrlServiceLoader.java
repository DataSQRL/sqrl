package com.datasqrl.util;

import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Service loads and instantiates the object of the generic type
 */
public class SqrlServiceLoader {

  public static <C extends ServiceLoadableClass<R>, R> Optional<R> loadService(Class<C> serviceClass, Object... args) {
    ServiceLoader<C> loader = ServiceLoader.load(serviceClass);
    Iterator<C> iterator = loader.iterator();
    C result = null;

    while (iterator.hasNext()) {
      C service = iterator.next();
      if (service.matches(args)) {
        if (result != null) {
          throw new IllegalStateException("Ambiguous entry found for service: " + serviceClass.getName());
        }
        result = service;
      }
    }

    if (result == null) {
      return Optional.empty();
    }

    return Optional.of(result.create());
  }
}
