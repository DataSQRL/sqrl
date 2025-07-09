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
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;

public class AbstractUseCaseTest extends AbstractAssetSnapshotTest {

  /*
  We snapshot the GraphQL schema only if it is not provided
   */
  private boolean hasGraphQL = false;

  protected AbstractUseCaseTest() {
    super(null);
  }

  void testUsecase(Path script, Path graphQLFile, Path packageFile) {
    assertThat(Files.exists(script)).isTrue();
    var baseDir = script.getParent();
    List<String> arguments = new ArrayList<>();
    arguments.add("compile");
    arguments.add(script.getFileName().toString());
    // Add optional GraphQL schema and package configuration if present
    if (graphQLFile != null) {
      hasGraphQL = true;
      assert Files.exists(graphQLFile);
      arguments.add(graphQLFile.getFileName().toString());
    }
    if (packageFile != null) {
      assert Files.exists(packageFile);
      arguments.add("-c");
      arguments.add(packageFile.getFileName().toString());
    }
    var testname =
        Stream.of(script, graphQLFile, packageFile)
            .map(AbstractAssetSnapshotTest::getDisplayName)
            .collect(Collectors.joining("-"));
    var hook = execute(baseDir, arguments);
    snapshot(testname, hook);
  }

  /**
   * Either snapshot the results in the plan and build directory (if successful) or the error
   * message (if it failed)
   *
   * @param testname
   * @param hook
   */
  public void snapshot(String testname, AssertStatusHook hook) {
    this.snapshot = Snapshot.of(testname, getClass());
    if (hook.isFailed()) {
      createMessageSnapshot(hook.getMessages());
    } else {
      createSnapshot();
    }
  }

  @Override
  public Predicate<Path> getBuildDirFilter() {
    return file ->
        file.getFileName().toString().equalsIgnoreCase("pipeline_explain.txt")
            || (!hasGraphQL && file.getFileName().toString().endsWith(".graphqls"));
  }

  @Override
  public Predicate<Path> getPlanDirFilter() {
    return path -> {
      if (path.getFileName().toString().equals("flink-sql-no-functions.sql")) {
        return true;
      }
      if (path.getFileName().toString().contains("flink")) {
        return false;
      }
      if (path.getFileName().toString().contains("schema")
          || path.getFileName().toString().contains("views")
          || List.of("kafka.json", "vertx.json").contains(path.getFileName().toString())) {
        return true;
      }
      return false;
    };
  }

  /**
   * Iterates over all SQRL scripts in a given directory (and sub-directories) and for each script,
   * finds all associated package.json and GraphQL schema (*.graphqls) files (i.e. they start with
   * the same name). All combinations of those files are a single test case.
   */
  @AllArgsConstructor
  public abstract static class SqrlScriptsAndLocalPackages implements ArgumentsProvider {

    private final Path directory;
    private final boolean includeFails;

    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations parameters, ExtensionContext context) {
      // Look for all package jsons
      return getSQRLScripts(directory, includeFails)
          .sorted(Comparator.comparing(p -> p.toFile().getName()))
          .flatMap(
              path -> {
                var pkgFiles = getPackageFiles(path.getParent());
                pkgFiles.sort(Comparator.comparing(p -> p.toFile().getName()));
                if (pkgFiles.isEmpty()) {
                  pkgFiles.add(null);
                }
                var graphQLFiles = getScriptGraphQLFiles(path);
                graphQLFiles.sort(Comparator.comparing(p -> p.toFile().getName()));
                if (graphQLFiles.isEmpty()) {
                  graphQLFiles.add(null);
                }
                return graphQLFiles.stream()
                    .flatMap(gql -> pkgFiles.stream().map(pkg -> Arguments.of(path, gql, pkg)));
              });
    }
  }
}
