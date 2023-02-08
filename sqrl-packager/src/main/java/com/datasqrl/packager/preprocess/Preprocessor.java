package com.datasqrl.packager.preprocess;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.flink.util.Preconditions;

public interface Preprocessor {

  Pattern getPattern();

  void loader(Path dir, ProcessorContext processorContext);

  @Getter
  public static class ProcessorContext {
    Set<Path> dependencies = new HashSet<>();

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
  }
}
