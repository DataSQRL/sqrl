package com.datasqrl.config;

import java.nio.file.Path;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import lombok.Getter;

@Getter
public class RootPath {

  private final Path rootDir;

  @Inject
  public RootPath(@Named("rootDir") Path rootDir) {
    this.rootDir = rootDir;
  }
}
