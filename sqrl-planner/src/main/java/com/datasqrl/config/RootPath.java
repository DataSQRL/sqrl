package com.datasqrl.config;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.nio.file.Path;
import lombok.Getter;
import lombok.experimental.Delegate;

@Getter
public class RootPath {

  private final Path rootDir;

  @Inject
  public RootPath(@Named("rootDir") Path rootDir) {
    this.rootDir = rootDir;
  }
}
