package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
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
    Set<Path> dependencies = new HashSet<>();
    Set<Path> libraries = new HashSet<>();

    Path rootDir;
    Path buildDir;

    public ProcessorContext(Path rootDir, Path buildDir) {
      this.rootDir = rootDir;
      this.buildDir = buildDir;
    }

    public void addDependency(Path dependency) {
      dependencies.add(dependency);
    }

    public void addDependencies(ProcessorContext context) {
      context.getDependencies().forEach(dep -> addDependency(dep));
    }

    public void addLibrary(Path jarPath) {
      libraries.add(jarPath);
    }
  }

}
