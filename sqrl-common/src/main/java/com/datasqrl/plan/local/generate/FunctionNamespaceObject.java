package com.datasqrl.plan.local.generate;

import com.datasqrl.name.Name;
import java.net.URL;
import java.util.Optional;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * Represents a {@link NamespaceObject} for a function.
 */
public interface FunctionNamespaceObject<T> extends NamespaceObject {

  Name getName();

  T getFunction();

  Optional<URL> getJarUrl();
}
