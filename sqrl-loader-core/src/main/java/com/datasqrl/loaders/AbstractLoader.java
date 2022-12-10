/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractLoader implements Loader {

  protected final Deserializer deserialize = new Deserializer();

  public static Path namepath2Path(Path basePath, NamePath path) {
    Path filePath = basePath;
    for (int i = 0; i < path.getNames().length; i++) {
      Name name = path.getNames()[i];
      filePath = filePath.resolve(name.getCanonical());
    }
    return filePath;
  }

  public static boolean isPackagePath(Path basePath, NamePath path) {
    return Files.isDirectory(AbstractLoader.namepath2Path(basePath,path));
  }

  @Override
  public Collection<Name> loadAll(LoaderContext ctx, NamePath basePath) {
    return getAllFilesInPath(namepath2Path(ctx.getPackagePath(), basePath)).stream()
        .map(p -> loadsFile(p))
        .filter(Optional::isPresent)
        .map(name -> {
          Name resovledName = Name.system(name.get());
          Preconditions.checkArgument(resovledName.getCanonical().equals(name.get()));
          boolean loaded = load(ctx, basePath.concat(resovledName), Optional.empty());
          Preconditions.checkArgument(loaded);
          return resovledName;
        })
        .collect(Collectors.toSet());
  }

  public static Set<Path> getAllFilesInPath(Path directory) {
    try (Stream<Path> files = Files.list(directory)) {
      return files.filter(Files::isRegularFile)
          .collect(Collectors.toSet());
    } catch (IOException e) {
      return Collections.EMPTY_SET;
    }
  }

}
