package com.datasqrl.packager;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.function.Predicate;
import lombok.Value;

@Value
class CopyFiles implements FileVisitor<Path> {

  Path srcDir;
  Path targetDir;
  Predicate<Path> copyFile;
  Collection<Path> excludedDirs;

  private boolean isExcludedDir(Path dir) throws IOException {
    for (Path p : excludedDirs) {
      if (dir.equals(p)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
      throws IOException {
    if (isExcludedDir(dir)) {
      return FileVisitResult.SKIP_SUBTREE;
    }
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
    if (copyFile.test(file)) {
      Packager.copyRelativeFile(file, srcDir, targetDir);
    }
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
    //TODO: collect error
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
    return FileVisitResult.CONTINUE;
  }
}
