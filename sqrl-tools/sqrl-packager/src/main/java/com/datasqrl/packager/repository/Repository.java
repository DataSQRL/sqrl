/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.repository;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.config.Dependency;

public interface Repository {

  public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException;

  public Optional<Dependency> resolveDependency(String packageName);

}
