package com.datasqrl.util;

import java.util.List;
import java.util.stream.Collectors;

public class ServiceLoaderException extends RuntimeException {

  final Class<?> clazz;
  final List<String> identifiers;

  public ServiceLoaderException(Class<?> clazz, String identifier) {
    this(clazz,List.of(identifier));
  }

  public ServiceLoaderException(Class<?> clazz, List<String> identifiers) {
    super(String.format("Could not load %s dependency for identifier(s): %s", clazz.getSimpleName(), String.join(",",identifiers)));
    this.clazz = clazz;
    this.identifiers = identifiers;
  }
}
