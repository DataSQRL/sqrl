package com.datasqrl.service;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import java.nio.file.Path;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class Build {

  ErrorCollector collector;

  @SneakyThrows
  public Path build(Packager packager) {
    packager.cleanUp();

    return packager.populateBuildDir(true);
  }
}
