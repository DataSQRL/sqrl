/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.io.file;

import com.datasqrl.util.BaseFileUtil;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.flink.connector.file.src.compression.StandardDeCompressors;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * Represents a file that is identified by a URI.
 *
 * <p>Depending on the URI scheme, this abstracts across multiple file systems including cloud
 * object storage.
 *
 * <p>This is implemented as a wrapper around Flink's {@link org.apache.flink.core.fs.FileSystem}.
 */
public class FilePath implements Serializable { // todo: move to io-core

  public static final Set<String> COMPRESSION_EXTENSIONS =
      StandardDeCompressors.getCommonSuffixes().stream()
          .map(String::toLowerCase)
          .collect(Collectors.toSet());

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
      try (var is = flinkPath.toUri().toURL().openStream()) {
        return new Status(false, is.available(), null, this);
      } catch (Throwable e) {
        return Status.NOT_EXIST;
      }
    } else {
      var status = getFileSystem().getFileStatus(flinkPath);
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
    var scheme = getScheme();
    return !Strings.isNullOrEmpty(scheme) && URL_SCHEMES.contains(getScheme());
  }

  public List<Status> listFiles() throws IOException {
    Preconditions.checkArgument(!isURL());
    var files = getFileSystem().listStatus(flinkPath);
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
    var fullName = getFileName();
    String compression = "", format = "";
    var extPair = BaseFileUtil.separateExtension(fullName);
    var extension = extPair.getRight();
    if (!Strings.isNullOrEmpty(extension) && COMPRESSION_EXTENSIONS.contains(extension)) {
      compression = extension;
      fullName = extPair.getLeft();
      extPair = BaseFileUtil.separateExtension(fullName);
      extension = extPair.getRight();
    }
    if (!Strings.isNullOrEmpty(extension)) {
      format = extension;
      fullName = extPair.getLeft();
    }
    // Match pattern
    var identifier = fullName;
    if (filenamePattern != null) {
      var matcher = filenamePattern.matcher(fullName);
      if (matcher.find()) {
        if (matcher.groupCount() > 0) {
          identifier = matcher.group(1);
          // If we expect an identifier, it cannot be empty
          if (Strings.isNullOrEmpty(identifier)) {
            return Optional.empty();
          }
        } else {
          return Optional.empty();
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
    var components = getComponents(null).get();
    if (components.hasCompression()) {
      is = StandardDeCompressors.getDecompressorForExtension(components.compression()).create(is);
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
    var filePath = (FilePath) o;
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

  public record NameComponents(
      String identifier, String fullName, String format, String compression) {

    public boolean hasFormat() {
      return !Strings.isNullOrEmpty(format);
    }

    public boolean hasCompression() {
      return !Strings.isNullOrEmpty(compression);
    }

    public String getSuffix() {
      var suffix = "";
      if (hasFormat()) {
        suffix += "." + format;
      }
      if (hasCompression()) {
        suffix += "." + compression;
      }
      return suffix;
    }
  }

  public record Status(boolean isDir, long length, Instant lastModified, FilePath path) {

    public static final Status NOT_EXIST = new Status(false, 0, null, null);

    Status(FileStatus status) {
      this(
          status.isDir(),
          status.getLen(),
          Instant.ofEpochMilli(status.getModificationTime()),
          new FilePath(status.getPath()));
    }

    public boolean exists() {
      return path != null;
    }
  }
}
