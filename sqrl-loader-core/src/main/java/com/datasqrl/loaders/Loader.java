/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;

public interface Loader {

  default boolean usesFile(Path file) {
    return loadsFile(file).isPresent();
  }

  boolean isPackage(Path packageBasePath, NamePath fullPath);

  Optional<String> loadsFile(Path file);

  boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias);

  Collection<Name> loadAll(LoaderContext ctx, NamePath basePath);

}
