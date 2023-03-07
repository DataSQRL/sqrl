/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.file;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.impl.file.DirectoryDataSystem.DirectoryConnector;
import com.datasqrl.io.tables.TableConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;

@Value
public class FilePathConfig implements Serializable {

  @NonNull
  List<FilePath> paths;
  boolean isDirectory;

  public static FilePathConfig of(String directoryURI, List<String> fileURIs,
      ErrorCollector errors) {
    if (!Strings.isNullOrEmpty(directoryURI)) {
      FilePath directoryPath = checkURI(directoryURI, true, errors, "directoryURI");
      if (directoryPath == null) {
        return null;
      }
      if (directoryPath.isURL()) {
        errors.fatal("directoryURI cannot be a URL: %s", directoryURI);
        return null;
      }
      if (!fileURIs.isEmpty()) {
        errors.warn("fileURIs are ignored since directoryURI is specified: [%s]", fileURIs);
      }
      return new FilePathConfig(List.of(directoryPath), true);
    } else if (!fileURIs.isEmpty()) {
      List<FilePath> filePaths = new ArrayList<>();
      for (String fileURI : fileURIs) {
        FilePath filePath = checkURI(fileURI, false, errors, "fileURIs");
        if (filePath == null) {
          return null;
        }
        filePaths.add(filePath);
      }
      //Verify minimal commonalities between paths
      Set<String> schemes = filePaths.stream().map(FilePath::getScheme)
          .collect(Collectors.toSet());
      if (schemes.size() > 1) {
        errors.fatal("fileURIs need to have one scheme but found: %s", schemes);
        return null;
      }
      return new FilePathConfig(filePaths, false);
    } else {
      errors.fatal("Need to specify directoryURI or fileURIs");
      return null;
    }
  }

  public static FilePathConfig ofDirectory(FilePath p) {
    return new FilePathConfig(List.of(p), true);
  }

  private static FilePath checkURI(String uri, boolean isDir, ErrorCollector errors,
      String fieldName) {
    FilePath filepath = new FilePath(uri);
    try {
      FilePath.Status status = filepath.getStatus();
      if (!status.exists() || status.isDir() != isDir) {
        errors.fatal("%s [%s] is not a %s", fieldName, uri, isDir ? "directory" : "file");
        return null;
      }
    } catch (IOException e) {
      errors.fatal("%s [%s] could not be accessed: %s", fieldName, uri, e);
      return null;
    }
    return filepath;
  }

  public FilePath getDirectory() {
    Preconditions.checkArgument(isDirectory);
    return Iterables.getOnlyElement(paths);
  }

  public boolean isDirectory() {
    return isDirectory;
  }

  public List<FilePath> getFiles() {
    Preconditions.checkArgument(!isDirectory);
    return paths;
  }

  public boolean isURL() {
    //We check scheme uniformity when constructing, so only accessing first path is valid
    return paths.get(0).isURL();
  }

  public List<FilePath> getFiles(DirectoryConnector directorySource,
      TableConfig table) {
    return paths.stream().filter(p -> directorySource.isTableFile(p, table))
        .collect(Collectors.toList());
  }

  public List<FilePath.Status> listFiles() throws IOException {
    if (isDirectory) {
      return getDirectory().listFiles();
    } else {
      List<FilePath.Status> result = new ArrayList<>();
      for (FilePath file : getFiles()) {
        result.add(file.getStatus());
      }
      return result;
    }
  }

}
