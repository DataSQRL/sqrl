package com.datasqrl.util;

import java.util.List;

public class ServiceLoaderException extends RuntimeException {

  final Class<?> clazz;
  final List<String> identifiers;

  public ServiceLoaderException(Class<?> clazz, String identifier) {
    this(clazz,List.of(identifier));
  }

  public ServiceLoaderException(Class<?> clazz, List<String> identifiers) {
    super("Could not load %s dependency for identifier(s): %s".formatted(clazz.getSimpleName(), String.join(",", identifiers)));
    this.clazz = clazz;
    this.identifiers = identifiers;
  }
}
