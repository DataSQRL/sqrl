/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.inmemory.io;

import com.datasqrl.io.impl.file.DirectoryDataSystem;
import com.datasqrl.io.impl.file.FilePath;
import com.datasqrl.io.impl.file.FilePathConfig;
import com.datasqrl.io.tables.TableConfig;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

public class FileStreamUtil {

  public static Stream<String> filesByline(Path... paths) {
    return filesByline(Arrays.stream(paths));
  }

  public static Stream<String> filesByline(Stream<Path> paths) {
    Preconditions.checkArgument(paths != null);
    return paths.flatMap(p -> {
      try {
        return Files.lines(p);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static Stream<String> readByLine(InputStream is) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    return reader.lines();
  }

  public static Stream<Path> matchingFiles(FilePathConfig pathConfig,
      DirectoryDataSystem.Connector directorySource,
      TableConfig table) throws IOException {
    if (pathConfig.isDirectory()) {
      return Files.find(FilePath.toJavaPath(pathConfig.getDirectory()), 100,
          (filePath, fileAttr) -> {
            if (!fileAttr.isRegularFile()) {
              return false;
            }
            if (fileAttr.size() <= 0) {
              return false;
            }
            return directorySource.isTableFile(FilePath.fromJavaPath(filePath), table);
          });
    } else {
      return pathConfig.getFiles(directorySource, table).stream().map(FilePath::toJavaPath);
    }
  }


}
