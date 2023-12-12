package com.datasqrl.util;

import com.datasqrl.canonicalizer.NamePath;
import java.nio.file.Path;
import java.util.List;

public class NameUtil {

  public static Path namepath2Path(Path basePath, NamePath path) {
    return createCanonicalPath(basePath, path.toStringList());
  }

  private static Path createCanonicalPath(Path basePath, List<String> pathList) {
    for (String path : pathList) {
      basePath = basePath.resolve(path.toLowerCase());
    }
    return basePath;
  }
}
