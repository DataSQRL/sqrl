package com.datasqrl.util;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;

public class NameUtil {

  public static Path namepath2Path(Path basePath, NamePath path) {
    Path filePath = basePath;
    for (int i = 0; i < path.getNames().length; i++) {
      Name name = path.getNames()[i];
      filePath = getCaseInsensitivePath(filePath, name.getDisplay());
    }
    return filePath;
  }

  @SneakyThrows
  public static Path getCaseInsensitivePath(Path filePath, String name) {
    //If path doesn't exist, resolve name provided
    if (!Files.exists(filePath)) {
      return filePath.resolve(name);
    }
    //check for exact match
    if (Files.exists(filePath.resolve(name))) {
      return filePath.resolve(name);
    }

    //find first inexact match, or else just the name provided
    return Files.list(filePath)
        .filter(path -> path.getFileName().toString().toLowerCase().equals(name))
        .findFirst()
        .orElse(filePath.resolve(name));
  }
}
