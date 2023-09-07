package com.datasqrl.util;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public abstract class ServiceLoaderFactory<T extends ServiceLoaderFactoryItem<T>> {
  private final List<T> implementations;

  public ServiceLoaderFactory(Class<T> serviceInterface) {
    this.implementations = ServiceLoader.load(serviceInterface)
        .stream()
        .map(ServiceLoader.Provider::get)
        .collect(Collectors.toList());
  }

  public List<T> findImplementations(Object... criteria) {
    return implementations.stream()
        .filter(impl -> impl.match(criteria))
        .map(impl -> impl.unbox())
        .collect(Collectors.toList());
  }

  public Optional<T> findImplementation(Object... criteria) {
    return implementations.stream()
        .filter(impl -> impl.match(criteria))
        .map(impl -> impl.unbox())
        .reduce((a, b) -> {
          throw new IllegalStateException("More than one matching implementation found");
        });
  }


  public interface Matchable<T> {
    boolean match(Object... criteria);

    default T unbox() {
      return (T) this;
    }
  }
}