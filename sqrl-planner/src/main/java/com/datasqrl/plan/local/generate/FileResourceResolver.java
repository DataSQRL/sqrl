package com.datasqrl.plan.local.generate;

import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.loaders.ResourceResolver;
import com.datasqrl.name.NamePath;
import com.google.common.base.Preconditions;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

public class FileResourceResolver implements ResourceResolver {

  Path baseDir;

  public FileResourceResolver(Path baseDir) {
    Preconditions.checkState(Files.isDirectory(baseDir));
    this.baseDir = baseDir;
  }

  //todo hacks remove
  public Optional<URL> resolveTableJson(NamePath namePath) {
    Path path = namepath2Path(baseDir, namePath.popLast());

    return resolve(path.resolve(namePath.getLast().getCanonical() + ".table.json"));
  }

  public Optional<URL> resolve(Path path) {
    try {
      return Optional.of(path.toFile().toURI().toURL());
    } catch (MalformedURLException e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  public List<URL> loadPath(NamePath namePath) {
    Path path = namepath2Path(baseDir, namePath);

    if (!Files.exists(path)) {
      return List.of();
    }

    return Files.list(path)
        .map(f-> {
          try {
            return f.toUri().toURL();
          } catch (MalformedURLException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }
}
