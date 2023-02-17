package com.datasqrl.plan.local.generate;

import com.datasqrl.loaders.ResourceResolver;
import com.datasqrl.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.datasqrl.util.NameUtil.namepath2Path;

public class FileResourceResolver implements ResourceResolver {

  Path baseDir;

  public FileResourceResolver(Path baseDir) {
    Preconditions.checkState(Files.isDirectory(baseDir));
    this.baseDir = baseDir;
  }

  //todo hacks remove
  public Optional<URI> resolveTableJson(NamePath namePath) {
    Path path = namepath2Path(baseDir, namePath.popLast());

    return resolve(path.resolve(namePath.getLast().getCanonical() + ".table.json"));
  }

  public Optional<URI> resolve(Path path) {
    return Optional.of(path.toFile().toURI());
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
}
