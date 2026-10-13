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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * Finds the package files of the project of a script, as they would be passed to the CLI, e.g.
 * {@code sqrl compile -c orders-shared-package.json -c orders-test-package.json}.
 *
 * <p>Package files are named freely, so they are the JSON files with "package" in their name, e.g.
 * {@code package.json} or {@code sensors_package_test.json}. A base package file names the main
 * script, and an overlay adds to a base, e.g. the configuration for tests. The project of a script
 * is the base whose main script it is, or else a base in the nearest directory above it with one.
 * Bases for local runs are preferred, since they need no cloud services, e.g. {@code
 * data_compaction_package_local.json} over {@code data_compaction_package_test.json}, which needs
 * credentials for an Iceberg catalog of AWS Glue. Next are bases for tests, since scripts are
 * checked with their test tables.
 *
 * <p>The overlays of a base are the package files without a main script whose names start like the
 * base's, e.g. {@code package-test-api.json} of {@code package.json}, or {@code
 * orders-test-package.json} of {@code orders-shared-package.json}. A base gets its overlay for
 * tests, or else for development, since a base may leave out what its overlays add, e.g. included
 * packages. The user can select another base for a script and its overlays, see {@link #candidates}
 * and {@link #overlays}.
 */
@Slf4j
final class ProjectPackages {

  private static final Pattern PACKAGE_FILE =
      Pattern.compile(".*package.*\\.json", Pattern.CASE_INSENSITIVE);
  private static final String SHARED = "shared";

  /**
   * The order in which overlays apply, from the general to the specific: shorter names first, e.g.
   * {@code package-deploy.json} before {@code package-deploy-staging.json}
   */
  private static final Comparator<Path> OVERLAY_ORDER =
      Comparator.comparingInt((Path file) -> file.getFileName().toString().length())
          .thenComparing(file -> file.getFileName().toString());

  /** The sections of a package file of DataSQRL, to tell it apart from other JSON files */
  /** The package files of the directories that were read, see {@link #packagesIn} */
  private static final Map<Path, Listing> LISTINGS = new ConcurrentHashMap<>();

  private static final List<String> SECTIONS =
      List.of("script", "engines", "enabled-engines", "connectors", "compiler", "test-runner");

  /**
   * A package file.
   *
   * @param file the file
   * @param main the main script of a base, or empty for an overlay
   */
  private record PackageFile(Path file, Optional<Path> main) {

    String name() {
      return file.getFileName().toString().toLowerCase(Locale.ROOT);
    }
  }

  private ProjectPackages() {}

  /**
   * Finds the package files of the project of a script.
   *
   * @param script the script
   * @param names the names of the package files of every project, if configured, which are then
   *     found upwards from the script instead
   * @return the package files, the first of which is the base, or empty if there is no project
   */
  static List<Path> find(Path script, List<String> names) {
    return find(script, names, Map.of());
  }

  /**
   * Finds the package files of the project of a script, or the ones that the user selected.
   *
   * @param script the script
   * @param names the names of the package files of every project, if configured
   * @param selected the package files that the user selected for main scripts: a base and any of
   *     its overlays, see {@link #candidates} and {@link #overlays}
   * @return the package files, the first of which is the base, or empty if there is no project
   */
  static List<Path> find(Path script, List<String> names, Map<Path, List<Path>> selected) {
    var chosen = selected.getOrDefault(script, List.of());
    if (!chosen.isEmpty() && chosen.get(0).getParent() != null) {
      var base = chosen.get(0);
      var isBase =
          packagesIn(base.getParent()).stream()
              .anyMatch(p -> p.file().equals(base) && p.main().isPresent());
      if (isBase) {
        var files = new ArrayList<Path>();
        files.add(base);
        chosen.stream()
            .skip(1)
            .filter(Files::isRegularFile)
            .distinct()
            .sorted(OVERLAY_ORDER)
            .forEach(files::add);
        return List.copyOf(files);
      }
    }
    for (var dir = script.getParent(); dir != null; dir = dir.getParent()) {
      var packages = names.isEmpty() ? detect(dir, script) : configured(dir, names);
      if (!packages.isEmpty()) {
        return packages;
      }
    }
    return List.of();
  }

  private static List<Path> configured(Path dir, List<String> names) {
    if (!Files.isRegularFile(dir.resolve(names.get(0)))) {
      return List.of();
    }
    return names.stream().map(dir::resolve).filter(Files::isRegularFile).toList();
  }

  private static List<Path> detect(Path dir, Path script) {
    var packages = packagesIn(dir);
    var bases = packages.stream().filter(p -> p.main().isPresent()).toList();
    var ofScript = bases.stream().filter(p -> p.main().get().equals(script)).toList();
    // A script that is not a main script is imported by one, which is in the nearest directory
    var candidates = ofScript.isEmpty() ? bases : ofScript;
    var base =
        candidates.stream()
            .min(
                Comparator.comparing((PackageFile p) -> !p.name().contains("local"))
                    .thenComparing(p -> !p.name().contains("test"))
                    .thenComparingInt(p -> p.name().length())
                    .thenComparing(PackageFile::name));
    return base.map(b -> withOverlay(b, packages)).orElse(List.of());
  }

  /**
   * The package files that name a script as their main script, e.g. one for local runs and one for
   * tests, of which the user may select one. They are in the nearest directory with any.
   */
  static List<Path> candidates(Path script) {
    for (var dir = script.getParent(); dir != null; dir = dir.getParent()) {
      var bases =
          packagesIn(dir).stream()
              .filter(p -> p.main().filter(script::equals).isPresent())
              .map(PackageFile::file)
              .toList();
      if (!bases.isEmpty()) {
        return bases;
      }
    }
    return List.of();
  }

  /** The overlays of a base, which the user may select, in the order in which they apply. */
  static List<Path> overlays(Path base) {
    var packages = packagesIn(base.getParent());
    return packages.stream()
        .filter(p -> p.file().equals(base))
        .findFirst()
        .map(b -> overlaysOf(b, packages).stream().map(PackageFile::file).toList())
        .orElse(List.of());
  }

  private static List<Path> withOverlay(PackageFile base, List<PackageFile> packages) {
    var files = new ArrayList<Path>();
    files.add(base.file());
    overlay(base, packages).ifPresent(overlay -> files.add(overlay.file()));
    return List.copyOf(files);
  }

  /** The overlay for tests, or else for development, that a base gets by default. */
  private static Optional<PackageFile> overlay(PackageFile base, List<PackageFile> packages) {
    var overlays =
        overlaysOf(base, packages).stream().filter(p -> !p.name().contains("prod")).toList();
    return overlays.stream()
        .filter(p -> p.name().contains("test"))
        .findFirst()
        .or(() -> overlays.stream().filter(p -> p.name().contains("dev")).findFirst());
  }

  /**
   * The overlays of a base: the package files without a main script whose names start like the
   * base's, i.e. its name without the extension, or of a shared base the part before {@code
   * shared}. They are in the order in which they apply.
   */
  private static List<PackageFile> overlaysOf(PackageFile base, List<PackageFile> packages) {
    var name = base.name();
    var shared = name.indexOf(SHARED);
    var prefix = shared >= 0 ? name.substring(0, shared) : name.replaceFirst("\\.json$", "");
    return packages.stream()
        .filter(p -> p.main().isEmpty() && p.name().startsWith(prefix))
        .sorted(Comparator.comparing(PackageFile::file, OVERLAY_ORDER))
        .toList();
  }

  /**
   * The package files of DataSQRL in a directory. Editors ask for the project of a file on every
   * change, so they are read again only when the directory or one of the files changed.
   */
  private static List<PackageFile> packagesIn(Path dir) {
    var modified = version(dir);
    var cached = LISTINGS.get(dir);
    if (cached != null && cached.isCurrent(modified)) {
      return cached.packages();
    }
    try (var files = Files.list(dir)) {
      var versions = new LinkedHashMap<Path, Version>();
      files
          .filter(file -> PACKAGE_FILE.matcher(file.getFileName().toString()).matches())
          .filter(Files::isRegularFile)
          .sorted()
          .forEach(file -> versions.put(file, version(file)));
      var packages = versions.keySet().stream().flatMap(file -> read(file).stream()).toList();
      LISTINGS.put(dir, new Listing(modified, versions, packages));
      return packages;
    } catch (IOException e) {
      log.debug("Could not list {}", dir, e);
      LISTINGS.remove(dir);
      return List.of();
    }
  }

  /** The package files of a directory, as of the versions of the directory and the files. */
  private record Listing(Version directory, Map<Path, Version> files, List<PackageFile> packages) {

    boolean isCurrent(Version current) {
      return current.equals(directory)
          && files.entrySet().stream().allMatch(e -> version(e.getKey()).equals(e.getValue()));
    }
  }

  /** The version of a file on disk, which changes when it is written. */
  private record Version(long modified, long size) {}

  private static Version version(Path file) {
    try {
      var attributes = Files.readAttributes(file, BasicFileAttributes.class);
      return new Version(attributes.lastModifiedTime().toMillis(), attributes.size());
    } catch (IOException e) {
      return new Version(-1, -1);
    }
  }

  private static Optional<PackageFile> read(Path file) {
    try {
      if (!(JsonParser.parseString(Files.readString(file)) instanceof JsonObject json)
          || SECTIONS.stream().noneMatch(json::has)) {
        return Optional.empty();
      }
      Optional<Path> main = Optional.empty();
      if (json.get("script") instanceof JsonObject script
          && script.get("main") != null
          && script.get("main").isJsonPrimitive()) {
        main = Optional.of(file.resolveSibling(script.get("main").getAsString()).normalize());
      }
      return Optional.of(new PackageFile(file, main));
    } catch (IOException | RuntimeException e) {
      // Not a package file, or one that is being edited
      return Optional.empty();
    }
  }
}
