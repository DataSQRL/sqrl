package com.datasqrl.config;

import java.nio.file.Path;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import lombok.Getter;

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
