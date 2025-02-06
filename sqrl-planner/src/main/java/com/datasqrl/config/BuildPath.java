package com.datasqrl.config;

import java.nio.file.Path;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import lombok.Getter;

@Getter
public class BuildPath {

  private final Path buildDir;

  @Inject
  public BuildPath(@Named("buildDir") Path buildDir) {
    this.buildDir = buildDir;
  }
}
