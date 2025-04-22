package com.datasqrl.discovery.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.datasqrl.io.file.FilePath;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.auto.service.AutoService;
import com.google.common.base.Strings;

import lombok.Value;

@Value
public class FileCompression {

  public static final Set<String> SUPPORTED_COMPRESSION_EXTENSIONS = FilePath.COMPRESSION_EXTENSIONS.stream()
      .map(String::toLowerCase).collect(Collectors.toUnmodifiableSet());

  public static Optional<CompressionIO> of(String compression) {
    if (Strings.isNullOrEmpty(compression)) {
        return Optional.of(new NoCompressionIO());
    }
    final var compressionLower = compression.toLowerCase();
    if (!SUPPORTED_COMPRESSION_EXTENSIONS.contains(compressionLower)) {
        return Optional.empty();
    }
    return ServiceLoaderDiscovery.findFirst(CompressionIO.class,
        cio -> cio.getExtensions().contains(compressionLower));
  }

  public interface CompressionIO {

    InputStream decompress(InputStream in) throws IOException;

    OutputStream compress(OutputStream out) throws IOException;

    Set<String> getExtensions();

  }

  private static class NoCompressionIO implements CompressionIO {

    @Override
    public InputStream decompress(InputStream in) throws IOException {
      return in;
    }

    @Override
    public OutputStream compress(OutputStream out) throws IOException {
      return out;
    }

    @Override
    public Set<String> getExtensions() {
      return Set.of();
    }
  }

  @AutoService(CompressionIO.class)
  public static class GzipCompressionIO implements CompressionIO {

    @Override
    public InputStream decompress(InputStream in) throws IOException {
      return new GZIPInputStream(in);
    }

    @Override
    public OutputStream compress(OutputStream out) throws IOException {
      return new GZIPOutputStream(out);
    }

    @Override
    public Set<String> getExtensions() {
      return Set.of("gzip", "gz");
    }
  }


}
