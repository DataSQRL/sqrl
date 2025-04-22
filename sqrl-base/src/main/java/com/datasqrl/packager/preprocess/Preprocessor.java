package com.datasqrl.packager.preprocess;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;

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

    /**
     * Adds to the 'lib' folder
     */
    public void addLibrary(Path jarPath) {
      libraries.add(jarPath);
    }
  }

}
