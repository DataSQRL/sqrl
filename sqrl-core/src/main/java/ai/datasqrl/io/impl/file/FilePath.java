package ai.datasqrl.io.impl.file;

import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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

  private static final int DELIMITER_CHAR = 46;

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
    FileStatus status = getFileSystem().getFileStatus(flinkPath);
    if (status == null) {
      return Status.NOT_EXIST;
    } else {
      return new Status(status);
    }
  }

  public List<Status> listFiles() throws IOException {
    FileStatus[] files = getFileSystem().listStatus(flinkPath);
    return Arrays.stream(files).map(Status::new).collect(Collectors.toList());
  }

  public boolean isDir() throws IOException {
    return getStatus().isDir();
  }

  public String getFileName() {
    return flinkPath.getName();
  }

  public NameComponents getComponents(Pattern partPattern) {
    String filename = getFileName();
    Pair<String, String> ext = separateExtension(filename);
    filename = ext.getLeft();
    String format = ext.getRight().toLowerCase();
    String compression;
    if (!Strings.isNullOrEmpty(format) && COMPRESSION_EXTENSIONS.contains(format)) {
      compression = format;
      ext = separateExtension(filename);
      filename = ext.getLeft();
      format = ext.getRight().toLowerCase();
    } else {
      compression = "";
    }
    //Match pattern
    String part = "";
    if (partPattern != null) {
      Matcher matcher = partPattern.matcher(filename);
      if (matcher.find()) {
        part = matcher.group(0);
        filename = filename.substring(0, filename.length() - part.length());
      }
    }
    return new NameComponents(filename, part, format, compression);
  }

  public InputStream read() throws IOException {
    return getFileSystem().open(flinkPath);
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


  @Value
  public static class NameComponents {

    private final String name;
    private final String part;
    private final String format;
    private final String compression;

    public boolean hasPart() {
      return !Strings.isNullOrEmpty(part);
    }

    public boolean hasFormat() {
      return !Strings.isNullOrEmpty(format);
    }

    public boolean hasCompression() {
      return !Strings.isNullOrEmpty(compression);
    }


  }

  public static Pair<String, String> separateExtension(String fileName) {
    if (Strings.isNullOrEmpty(fileName)) {
      return null;
    }
    int offset = fileName.lastIndexOf(DELIMITER_CHAR);
    if (offset == -1) {
      return Pair.of(fileName, "");
    } else {
      return Pair.of(fileName.substring(0, offset).trim(), fileName.substring(offset + 1).trim());
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
