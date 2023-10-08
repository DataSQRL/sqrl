package com.datasqrl.util;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;

public class NameUtil {

  public static Path namepath2Path(Path basePath, NamePath path) {
    return resolveCaseInsensitive(basePath, path.toStringList())
        .orElseGet(()->createEmptyPath(basePath, path.toStringList()));
  }

  private static Path createEmptyPath(Path basePath, List<String> pathList) {
    for (String path : pathList) {
      basePath = basePath.resolve(path);
    }
    return basePath;
  }

  public static Optional<Path> resolveCaseInsensitive(Path basePath, List<String> parts) {
    Path currentPath = basePath;
    for (String part : parts) {
      Optional<Path> nextPath = findCaseInsensitive(currentPath, part);
      if (nextPath.isPresent()) {
        currentPath = nextPath.get();
      } else {
        return Optional.empty();
      }
    }
    return Optional.of(currentPath);
  }

  private static Optional<Path> findCaseInsensitive(Path dir, String name) {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path entry : stream) {
        if (entry.getFileName().toString().equalsIgnoreCase(name)) {
          return Optional.of(entry);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Optional.empty();
  }
}
