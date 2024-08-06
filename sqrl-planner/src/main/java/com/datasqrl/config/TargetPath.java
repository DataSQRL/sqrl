package com.datasqrl.config;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.nio.file.Path;
import lombok.Getter;
import lombok.experimental.Delegate;

@Getter
public class TargetPath {

  private final Path targetDir;

  @Inject
  public TargetPath(@Named("targetDir") Path targetDir) {
    this.targetDir = targetDir;
  }
}
