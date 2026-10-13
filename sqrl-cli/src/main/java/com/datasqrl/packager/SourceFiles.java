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
package com.datasqrl.packager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Tracks the source files that the {@link FilePreprocessingPipeline} copies into the build
 * directory. Source files can be overlaid with content that is not saved to disk yet (e.g. editor
 * buffers of a language server), and every build file remembers the source file it came from, so
 * that errors in build files can be attributed to the source files.
 */
@Component
public class SourceFiles {

  private final Map<Path, String> overlays = new ConcurrentHashMap<>();
  private final Map<Path, Path> buildToSource = new ConcurrentHashMap<>();

  /** Replaces the content of the given source files with the given content when they are read. */
  public void setOverlays(Map<Path, String> overlaysBySourcePath) {
    overlays.clear();
    overlaysBySourcePath.forEach((path, content) -> overlays.put(normalize(path), content));
  }

  /** The overlaid content of a source file, if any. */
  public Optional<String> getOverlay(Path sourceFile) {
    return Optional.ofNullable(overlays.get(normalize(sourceFile)));
  }

  /** Reads a source file, preferring its overlaid content. */
  public String read(Path sourceFile) throws IOException {
    var overlay = getOverlay(sourceFile);
    return overlay.isPresent() ? overlay.get() : Files.readString(sourceFile);
  }

  /** Records that the build file was created from the source file. */
  public void recordBuildFile(Path buildFile, Path sourceFile) {
    buildToSource.put(normalize(buildFile), normalize(sourceFile));
  }

  /** The source file a build file was created from, if it was created by the pipeline. */
  public Optional<Path> getSourceFile(Path buildFile) {
    return Optional.ofNullable(buildToSource.get(normalize(buildFile)));
  }

  /** The source files that were copied or rendered into the build directory. */
  public Set<Path> getSourceFiles() {
    return Set.copyOf(buildToSource.values());
  }

  public void clearBuildFiles() {
    buildToSource.clear();
  }

  private static Path normalize(Path path) {
    return path.toAbsolutePath().normalize();
  }
}
