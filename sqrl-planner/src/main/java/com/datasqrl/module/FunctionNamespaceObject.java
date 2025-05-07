package com.datasqrl.module;

import java.net.URL;
import java.util.Optional;

import com.datasqrl.canonicalizer.Name;

/**
 * Represents a {@link NamespaceObject} for a function.
 */
public interface FunctionNamespaceObject<T> extends NamespaceObject {

  @Override
  Name getName();

  T getFunction();

  Optional<URL> getJarUrl();
}
