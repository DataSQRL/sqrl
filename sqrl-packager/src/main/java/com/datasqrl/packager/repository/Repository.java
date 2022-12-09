package com.datasqrl.packager.repository;

import com.datasqrl.packager.config.Dependency;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public interface Repository {

  public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException;

  public Optional<Dependency> resolveDependency(String packageName);

}
