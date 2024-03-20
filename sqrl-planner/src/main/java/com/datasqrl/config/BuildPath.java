package com.datasqrl.config;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.nio.file.Path;
import lombok.experimental.Delegate;

public class BuildPath implements Path {

  @Delegate
  private final Path buildDir;

  @Inject
  public BuildPath(@Named("buildDir") Path buildDir) {
    this.buildDir = buildDir;
  }
}
