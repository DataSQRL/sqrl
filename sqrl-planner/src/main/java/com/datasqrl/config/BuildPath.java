package com.datasqrl.config;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.nio.file.Path;
import lombok.Getter;
import lombok.experimental.Delegate;

@Getter
public class BuildPath {

  public static final String UDF_DIR = "lib";

  private final Path buildDir;

  @Inject
  public BuildPath(@Named("buildDir") Path buildDir) {
    this.buildDir = buildDir;
  }

  public Path getUdfPath() {
    return buildDir.resolve(BuildPath.UDF_DIR);
  }
}
