package com.datasqrl.module.resolver;

import static com.datasqrl.util.NameUtil.namepath2Path;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.datasqrl.canonicalizer.NamePath;
import com.google.common.base.Preconditions;

import lombok.SneakyThrows;

public class FileResourceResolver implements ResourceResolver {

  Path baseDir;

  public FileResourceResolver(Path baseDir) {
    Preconditions.checkState(Files.isDirectory(baseDir));
    this.baseDir = baseDir;
  }

  @Override
  public String toString() {
    return "FileResourceResolver[" + baseDir + ']';
  }

  @SneakyThrows
  @Override
  public List<Path> loadPath(NamePath namePath) {
    Path path = namepath2Path(baseDir, namePath);

    if (!Files.exists(path)) {
      return List.of();
    }

    return Files.list(path)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<Path> resolveFile(NamePath namePath) {
    var path = namepath2Path(baseDir, namePath);
    if (!Files.exists(path)) {
      return Optional.empty();
    }
    return Optional.of(path);
  }
}
