package com.datasqrl.util;

import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import java.nio.file.Path;

public class NameUtil {
  public static Path namepath2Path(Path basePath, NamePath path) {
    Path filePath = basePath;
    for (int i = 0; i < path.getNames().length; i++) {
      Name name = path.getNames()[i];
      filePath = filePath.resolve(name.getCanonical());
    }
    return filePath;
  }
}
