package com.datasqrl.module;

import com.datasqrl.canonicalizer.Name;
import java.net.URL;
import java.util.Optional;

/**
 * Represents a {@link NamespaceObject} for a function.
 */
public interface FunctionNamespaceObject<T> extends NamespaceObject {

  Name getName();

  T getFunction();

  Optional<URL> getJarUrl();
}
