package com.datasqrl.plan;

import java.nio.file.Path;
import java.util.Optional;

public interface MainScript {
  Optional<Path> getPath();
  String getContent();
}
