/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.file;

import com.datasqrl.io.formats.FileFormatExtension;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.connector.file.src.compression.StandardDeCompressors;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * Represents a file that is identified by a URI.
 * <p>
 * Depending on the URI scheme, this abstracts across multiple file systems including cloud object
 * storage.
 * <p>
 * This is implemented as a wrapper around Flink's {@link org.apache.flink.core.fs.FileSystem}.
 */
public class FilePath implements Serializable {

  private static final Set<String> COMPRESSION_EXTENSIONS = StandardDeCompressors.getCommonSuffixes()
      .stream()
      .map(String::toLowerCase).collect(Collectors.toSet());

  private static final Set<String> URL_SCHEMES = Set.of("http", "https");


  private final Path flinkPath;

  public FilePath(@NonNull String uri) {
    this(new Path(uri));
  }

  private FilePath(@NonNull Path path) {
    flinkPath = path;
  }

  public FilePath(@NonNull FileStatus file) {
    flinkPath = file.getPath();
  }

  public Status getStatus() throws IOException {
    if (isURL()) {
      try (InputStream is = flinkPath.toUri().toURL().openStream()) {
        return new Status(false, is.available(), null, this);
      } catch (Throwable e) {
        return Status.NOT_EXIST;
      }
    } else {
      FileStatus status = getFileSystem().getFileStatus(flinkPath);
      if (status == null) {
        return Status.NOT_EXIST;
      } else {
        return new Status(status);
      }
    }
  }

  public String getScheme() {
    return flinkPath.toUri().getScheme();
  }

  public boolean isURL() {
    String scheme = getScheme();
    return !Strings.isNullOrEmpty(scheme) && URL_SCHEMES.contains(getScheme());
  }

  public List<Status> listFiles() throws IOException {
    Preconditions.checkArgument(!isURL());
    FileStatus[] files = getFileSystem().listStatus(flinkPath);
    return Arrays.stream(files).map(Status::new).collect(Collectors.toList());
  }

  public boolean isDir() throws IOException {
    return getStatus().isDir();
  }

  public String getFileName() {
    return flinkPath.getName();
  }

  public FilePath resolve(String sub) {
    return new FilePath(new Path(this.flinkPath, sub));
  }

  public Optional<NameComponents> getComponents(Pattern filenamePattern) {
    String fullName = getFileName();
    String compression = "", format = "";
    Pair<String, String> extPair = FileUtil.separateExtension(fullName);
    String extension = extPair.getRight();
    if (!Strings.isNullOrEmpty(extension) && COMPRESSION_EXTENSIONS.contains(extension)) {
      compression = extension;
      fullName = extPair.getLeft();
      extPair = FileUtil.separateExtension(fullName);
      extension = extPair.getRight();
    }
    if (!Strings.isNullOrEmpty(extension) && FileFormatExtension.validFormat(extension)) {
      format = extension;
      fullName = extPair.getLeft();
    }
    //Match pattern
    String identifier = fullName;
    if (filenamePattern != null) {
      Matcher matcher = filenamePattern.matcher(fullName);
      if (matcher.find()) {
        if (matcher.groupCount() > 0) {
          identifier = matcher.group(1);
          //If we expect an identifier, it cannot be empty
          if (Strings.isNullOrEmpty(identifier)) {
            return Optional.empty();
          }
        } else {
          identifier = "";
        }
      } else {
        return Optional.empty();
      }
    } else if (Strings.isNullOrEmpty(identifier)) {
      return Optional.empty();
    }
    return Optional.of(new NameComponents(identifier, fullName, format, compression));
  }

  public InputStream read() throws IOException {
    InputStream is;
    if (isURL()) {
      is = flinkPath.toUri().toURL().openStream();
    } else {
      is = getFileSystem().open(flinkPath);
    }
    NameComponents components = getComponents(null).get();
    if (components.hasCompression()) {
      is = StandardDeCompressors.getDecompressorForExtension(components.getCompression())
          .create(is);
    }
    return is;
  }

  public OutputStream write() throws IOException {
    return getFileSystem().create(flinkPath, FileSystem.WriteMode.OVERWRITE);
  }

  private FileSystem getFileSystem() throws IOException {
    return flinkPath.getFileSystem();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilePath filePath = (FilePath) o;
    return filePath.flinkPath.equals(flinkPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(flinkPath);
  }

  @Override
  public String toString() {
    return flinkPath.toString();
  }

  public static Path toFlinkPath(FilePath path) {
    return path.flinkPath;
  }

  public static FilePath fromFlinkPath(Path path) {
    return new FilePath(path);
  }

  public static FilePath fromJavaPath(java.nio.file.Path path) {
    return new FilePath(path.toAbsolutePath().toString());
  }

  public static java.nio.file.Path toJavaPath(FilePath path) {
    return java.nio.file.Path.of(path.flinkPath.toString());
  }


  @Value
  public static class NameComponents {

    private final String identifier;
    private final String fullName;
    private final String format;
    private final String compression;

    public boolean hasFormat() {
      return !Strings.isNullOrEmpty(format);
    }

    public boolean hasCompression() {
      return !Strings.isNullOrEmpty(compression);
    }


  }


  @Value
  @AllArgsConstructor
  public static class Status {

    public static final Status NOT_EXIST = new Status(false, 0, null, null);

    private final boolean isDir;
    private final long length;
    private final Instant lastModified;
    private final FilePath path;

    Status(FileStatus status) {
      this(status.isDir(), status.getLen(),
          Instant.ofEpochMilli(status.getModificationTime()),
          new FilePath(status.getPath()));
    }

    public boolean exists() {
      return path != null;
    }

  }

}
