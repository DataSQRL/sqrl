package com.datasqrl.packager.preprocess;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Getter;

public interface Preprocessor {

  Pattern getPattern();

  void loader(Path dir, ProcessorContext processorContext, ErrorCollector errors);

  @Getter
  public static class ProcessorContext {

    Set<Path> dependencies, libraries;
    Path rootDir, buildDir;

    SqrlConfig sqrlConfig;
    Optional<NamePath> name = Optional.empty();

    public ProcessorContext(Path rootDir, Path buildDir, SqrlConfig sqrlConfig) {
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

    /**
     * Instead of the usual module folder, it creates a differenly named one
     */
    public ProcessorContext createModuleFolder(NamePath name) {
      this.name = Optional.of(name);
      return this;
    }
  }

  public static String toRegex(String filename) {
    return filename.replace(".","\\.");
  }

}
