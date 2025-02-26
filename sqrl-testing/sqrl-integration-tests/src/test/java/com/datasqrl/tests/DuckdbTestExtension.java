package com.datasqrl.tests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;

public class DuckdbTestExtension implements TestExtension {
  Path path = Path.of("/tmp/duckdb");

  private void deleteDirectory(Path directory) throws IOException {
    Files.walk(directory)
        .sorted(
            (path1, path2) -> path2.compareTo(path1)) // Sort in reverse order to delete files first
        .forEach(
            path -> {
              try {
                Files.delete(path);
              } catch (IOException e) {
                throw new RuntimeException("Failed to delete " + path, e);
              }
            });
  }

  @SneakyThrows
  public void createDir() {

    Files.createDirectories(path);
  }

  @Override
  public void setup() {
    try {
      deleteDirectory(path);
    } catch (Exception e) {
    }

    createDir();
  }

  @Override
  public void teardown() {
    try {
      deleteDirectory(path);
    } catch (Exception e) {
    }
  }
}
