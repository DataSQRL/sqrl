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
package com.datasqrl.lsp;

import com.datasqrl.loaders.ModuleLoaderImpl;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The documents that are open in the editor. Their content replaces the files on disk when a
 * project is checked, since the editor may not have saved them yet.
 */
class OpenDocuments {

  private final Map<Path, String> contents = new ConcurrentHashMap<>();
  private final Map<Path, String> uris = new ConcurrentHashMap<>();

  void open(Path file, String uri, String text) {
    uris.put(file, uri);
    contents.put(file, text);
  }

  void update(Path file, String text) {
    contents.put(file, text);
  }

  void close(Path file) {
    contents.remove(file);
  }

  /** The content of all open documents by file. */
  Map<Path, String> snapshot() {
    return Map.copyOf(contents);
  }

  /** The content of a file, from the editor if it is open, otherwise from disk. */
  Optional<String> read(Path file) {
    var open = contents.get(file);
    if (open != null) {
      return Optional.of(open);
    }
    try {
      return Optional.of(Files.readString(file));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  /** The URI the editor uses for a file, so that diagnostics are published under the same URI. */
  String uri(Path file) {
    return uris.computeIfAbsent(file, f -> f.toUri().toString());
  }

  /** Whether a file is a SQRL script, which language features are for. */
  static boolean isScript(Path file) {
    return file.toString().endsWith(ModuleLoaderImpl.SQRL_FILE_EXTENSION);
  }

  /** The file of a {@code file:} URI, or empty for other URIs. */
  static Optional<Path> toPath(String uri) {
    try {
      var parsed = URI.create(uri);
      if (!"file".equalsIgnoreCase(parsed.getScheme())) {
        return Optional.empty();
      }
      return Optional.of(Path.of(parsed).toAbsolutePath().normalize());
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
