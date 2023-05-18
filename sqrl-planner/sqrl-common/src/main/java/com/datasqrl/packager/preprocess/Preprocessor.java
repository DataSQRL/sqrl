package com.datasqrl.packager.preprocess;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.HashSet;
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

    public ProcessorContext(Path rootDir, Path buildDir, SqrlConfig sqrlConfig) {
      this.rootDir = rootDir;
      this.buildDir = buildDir;
      this.dependencies = new HashSet<>();
      this.libraries = new HashSet<>();
      this.sqrlConfig = sqrlConfig;
    }

    public void addDependency(Path dependency) {
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
