package com.datasqrl.util;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.nio.file.Path;

public class NameUtil {

  public static Path namepath2Path(Path basePath, NamePath path) {
    Path filePath = basePath;
    for (int i = 0; i < path.getNames().length; i++) {
      Name name = path.getNames()[i];
      filePath = filePath.resolve(name.getDisplay());
    }
    return filePath;
  }
}
