/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.packager.preprocessor;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Getter;

public interface Preprocessor {

  Pattern getPattern();

  void processFile(Path dir, ProcessorContext processorContext, ErrorCollector errors);

  @Getter
  @AllArgsConstructor
  class ProcessorContext {

    Set<Path> dependencies, libraries;
    Path rootDir, buildDir;

    PackageJson sqrlConfig;
    Optional<NamePath> name = Optional.empty();

    public ProcessorContext(Path rootDir, Path buildDir, PackageJson sqrlConfig) {
      this.rootDir = rootDir;
      this.buildDir = buildDir;
      this.dependencies = new HashSet<>();
      this.libraries = new HashSet<>();
      this.sqrlConfig = sqrlConfig;
    }

    public void addDependency(Path dependency) {
      Preconditions.checkState(Files.isRegularFile(dependency), "Dependency must be a file");
      dependencies.add(dependency);
    }

    public void addDependencies(ProcessorContext context) {
      dependencies.addAll(context.getDependencies());
    }

    /** Adds to the 'lib' folder */
    public void addLibrary(Path jarPath) {
      libraries.add(jarPath);
    }
  }
}
