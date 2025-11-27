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

import static com.datasqrl.config.SqrlConstants.GRAPHQL_SCHEMA_EXTENSION;
import static com.datasqrl.config.SqrlConstants.SQRL_EXTENSION;

import com.datasqrl.AbstractAssetSnapshotTest.TestNameModifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ArgumentsProviders {

  private static final String PACKAGE_JSON_REGEX = "package.*\\.json";

  @AllArgsConstructor
  public abstract static class PackageProvider implements ArgumentsProvider {

    private final List<Path> directories;

    public PackageProvider(Path... directories) {
      this(List.of(directories));
    }

    protected String packageJsonRegex() {
      return PACKAGE_JSON_REGEX;
    }

    protected Function<Path, Boolean> testModifierFilter() {
      return path -> TestNameModifier.of(path.getParent()) != TestNameModifier.disabled;
    }

    protected Stream<Path> collectPackageJsonFiles() {
      return directories.stream()
          .flatMap(p -> collectPackageFiles(p, packageJsonRegex(), testModifierFilter()))
          .sorted();
    }

    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations params, ExtensionContext ctx) {

      return collectPackageJsonFiles().map(Arguments::of);
    }
  }

  @AllArgsConstructor
  public abstract static class SqrlScriptProvider implements ArgumentsProvider {

    Path directory;
    boolean includeFail;

    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations params, ExtensionContext ctx) {

      return collectSqrlScripts(directory, false, includeFail)
          .sorted(Comparator.comparing(c -> c.toFile().getName().toLowerCase()))
          .map(Arguments::of);
    }
  }

  @AllArgsConstructor
  public abstract static class GraphQLSchemaProvider implements ArgumentsProvider {

    Path directory;
    boolean includeFail;

    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations params, ExtensionContext ctx) {

      return collectGraphQLSchemas(directory, false, includeFail)
          .sorted(Comparator.comparing(c -> c.toFile().getName().toLowerCase()))
          .map(Arguments::of);
    }
  }

  @AllArgsConstructor
  public abstract static class FileProvider implements ArgumentsProvider {

    Path directory;
    String extension;

    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations params, ExtensionContext ctx) {

      return getCollectedFiles().map(Arguments::of);
    }

    protected Stream<Path> getCollectedFiles() {
      return collectFiles(directory, false)
          .filter(p -> p.getFileName().toString().toLowerCase().endsWith(extension.toLowerCase()))
          .sorted();
    }
  }

  // Helper functions

  public static Stream<Path> collectPackageFiles(
      Path dir, String packageJsonRegex, Function<Path, Boolean> testModifierFilter) {

    return collectFiles(dir, true)
        .filter(path -> path.getFileName().toString().matches(packageJsonRegex))
        .filter(testModifierFilter::apply)
        .sorted();
  }

  public static Stream<Path> collectSqrlScripts(
      Path dir, boolean includeNested, boolean includeFail) {

    return collectFiles(dir, includeNested)
        .filter(path -> path.toString().endsWith('.' + SQRL_EXTENSION))
        .filter(
            path -> !path.getFileName().toString().equalsIgnoreCase("sources." + SQRL_EXTENSION))
        .filter(path -> ArgumentsProviders.includeNested(path, includeFail))
        .sorted();
  }

  public static Stream<Path> collectGraphQLSchemas(
      Path dir, boolean includeNested, boolean includeFail) {

    return collectFiles(dir, includeNested)
        .filter(path -> path.toString().endsWith('.' + GRAPHQL_SCHEMA_EXTENSION))
        .filter(path -> ArgumentsProviders.includeNested(path, includeFail))
        .sorted();
  }

  @SneakyThrows
  public static Stream<Path> collectFiles(Path dir, boolean includeNested) {
    return Files.walk(dir, includeNested ? Integer.MAX_VALUE : 1)
        .filter(path -> !Files.isDirectory(path))
        .filter(path -> !path.toString().contains("/build/"));
  }

  private static boolean includeNested(Path path, boolean includeFail) {
    var mod = TestNameModifier.of(path);
    return mod == TestNameModifier.none
        || (includeFail && (mod == TestNameModifier.fail || mod == TestNameModifier.warn));
  }
}
