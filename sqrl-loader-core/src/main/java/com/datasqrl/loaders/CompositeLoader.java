/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class CompositeLoader extends AbstractLoader implements Loader {

  List<Loader> loaders;

  public CompositeLoader(Loader... loaders) {
    this(List.of(loaders));
  }

  @Override
  public boolean isPackage(Path packageBasePath, NamePath fullPath) {
    for (Loader loader : loaders) {
      if (loader.isPackage(packageBasePath, fullPath)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Optional<String> loadsFile(Path file) {
    for (Loader loader : loaders) {
      Optional<String> result = loader.loadsFile(file);
      if (result.isPresent()) {
        return result;
      }
    }
    return Optional.empty();
  }

  @Override
  public boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias) {
    for (Loader loader : loaders) {
      if (loader.load(ctx, fullPath, alias)) {
        return true;
      }
    }
    return false;
  }

}
