package com.datasqrl.module.resolver;

import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class FileResourceResolver implements ResourceResolver {

  Path baseDir;

  public FileResourceResolver(Path baseDir) {
    Preconditions.checkState(Files.isDirectory(baseDir));
    this.baseDir = baseDir;
  }

  public Optional<URI> resolve(Path path) {
    return Optional.of(path.toFile().toURI());
  }

  @Override
  public String toString() {
    return "FileResourceResolver[" + baseDir + ']';
  }

  @SneakyThrows
  @Override
  public List<URI> loadPath(NamePath namePath) {
    Path path = namepath2Path(baseDir, namePath);

    if (!Files.exists(path)) {
      return List.of();
    }

    return Files.list(path)
        .map(Path::toUri)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<URI> resolveFile(NamePath namePath) {
    Path path = namepath2Path(baseDir, namePath);
    if (!Files.exists(path)) {
      return Optional.empty();
    }
    return Optional.of(path.toUri());
  }

  @Override
  public Path getResourcePath(NamePath path) {
    Path p = baseDir;
    for (Name element : path) {
      p = p.resolve(element.getDisplay());
    }
    return p;
  }
}
