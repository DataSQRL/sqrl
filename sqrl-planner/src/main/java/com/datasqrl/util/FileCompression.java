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
package com.datasqrl.util;

import com.datasqrl.io.file.FilePath;
import com.google.auto.service.AutoService;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class FileCompression {

  public static final Set<String> SUPPORTED_COMPRESSION_EXTENSIONS =
      FilePath.COMPRESSION_EXTENSIONS.stream()
          .map(String::toLowerCase)
          .collect(Collectors.toUnmodifiableSet());

  public static Optional<CompressionIO> of(String compression) {
    if (Strings.isNullOrEmpty(compression)) {
      return Optional.of(new NoCompressionIO());
    }
    final var compressionLower = compression.toLowerCase();
    if (!SUPPORTED_COMPRESSION_EXTENSIONS.contains(compressionLower)) {
      return Optional.empty();
    }
    return ServiceLoaderDiscovery.findFirst(
        CompressionIO.class, cio -> cio.getExtensions().contains(compressionLower));
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
