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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Finds the package files of projects laid out like the DataSQRL examples. */
class ProjectPackagesTest {

  @TempDir Path dir;

  @Test
  void givenPackageFileWithAnotherName_whenFind_thenFindsIt() throws IOException {
    var script = script("baseball_card.sqrl");
    base("baseball_card_test_package.json", "baseball_card.sqrl");

    assertThat(find(script)).containsExactly("baseball_card_test_package.json");
  }

  @Test
  void givenPackagesForLocalRunsAndTests_whenFind_thenPrefersTheLocalOne() throws IOException {
    var script = script("data_compaction.sqrl");
    base("data_compaction_package_test.json", "data_compaction.sqrl");
    base("data_compaction_package_local.json", "data_compaction.sqrl");

    assertThat(find(script)).containsExactly("data_compaction_package_local.json");
  }

  @Test
  void givenSeveralPackagesOfTheScript_whenFind_thenPrefersTheOneForTests() throws IOException {
    var script = script("sensors.sqrl");
    base("sensors_package_api.json", "sensors.sqrl");
    base("sensors_package_test.json", "sensors.sqrl");

    assertThat(find(script)).containsExactly("sensors_package_test.json");
  }

  @Test
  void givenSharedPackage_whenFind_thenAddsItsOverlayForTests() throws IOException {
    var script = script("orders.sqrl");
    base("orders-shared-package.json", "orders.sqrl");
    overlay("orders-dev-package.json");
    overlay("orders-prod-package.json");
    overlay("orders-test-package.json");

    assertThat(find(script))
        .containsExactly("orders-shared-package.json", "orders-test-package.json");
  }

  @Test
  void givenScriptsWithTheirOwnPackagesInOneDirectory_whenFind_thenFindsThePackageOfEach()
      throws IOException {
    var compaction = script("data_compaction.sqrl");
    var generator = script("datagen.sqrl");
    base("data_compaction_package_local.json", "data_compaction.sqrl");
    base("data_compaction_package_test.json", "data_compaction.sqrl");
    overlay("data_compaction_package_prod_test.json");
    base("datagen_package_test.json", "datagen.sqrl");

    assertThat(find(compaction)).containsExactly("data_compaction_package_local.json");
    assertThat(find(generator)).containsExactly("datagen_package_test.json");
  }

  @Test
  void givenSelectedPackageFile_whenFind_thenUsesItInsteadOfThePreferredOne() throws IOException {
    var script = script("data_compaction.sqrl");
    base("data_compaction_package_local.json", "data_compaction.sqrl");
    base("data_compaction_package_test.json", "data_compaction.sqrl");
    base("datagen_package_test.json", "datagen.sqrl");
    var local = dir.resolve("data_compaction_package_local.json");
    var test = dir.resolve("data_compaction_package_test.json");

    assertThat(ProjectPackages.candidates(script)).containsExactly(local, test);
    assertThat(find(script)).containsExactly("data_compaction_package_local.json");
    assertThat(ProjectPackages.find(script, List.of(), Map.of(script, List.of(test))))
        .containsExactly(test);
    // A selected file that is no longer a package file of a main script is ignored
    assertThat(
            ProjectPackages.find(
                script, List.of(), Map.of(script, List.of(dir.resolve("gone.json")))))
        .containsExactly(local);
  }

  @Test
  void givenBaseWithOverlays_whenFind_thenAddsTheOverlayForTests() throws IOException {
    // As in a project that is compiled with e.g. `package.json package-test-api.json`
    var script = script("metrics.sqrl");
    base("package.json", "metrics.sqrl");
    overlay("package-test-api.json");
    overlay("package-test-static.json");
    overlay("package-deploy.json");
    overlay("package-deploy-prod.json");
    overlay("package-catchup.json");

    assertThat(find(script)).containsExactly("package.json", "package-test-api.json");
    assertThat(ProjectPackages.overlays(dir.resolve("package.json")))
        .extracting(file -> file.getFileName().toString())
        .containsExactly(
            "package-deploy.json",
            "package-catchup.json",
            "package-test-api.json",
            "package-deploy-prod.json",
            "package-test-static.json");
  }

  @Test
  void givenSelectedOverlays_whenFind_thenAppliesThemFromTheGeneralToTheSpecific()
      throws IOException {
    var script = script("metrics.sqrl");
    base("package.json", "metrics.sqrl");
    overlay("package-deploy.json");
    overlay("package-deploy-staging.json");
    var selected =
        List.of(
            dir.resolve("package.json"),
            dir.resolve("package-deploy-staging.json"),
            dir.resolve("package-deploy.json"));

    assertThat(ProjectPackages.find(script, List.of(), Map.of(script, selected)))
        .containsExactly(
            dir.resolve("package.json"),
            dir.resolve("package-deploy.json"),
            dir.resolve("package-deploy-staging.json"));
  }

  @Test
  void givenImportedScriptInSubdirectory_whenFind_thenFindsThePackageAbove() throws IOException {
    var helper = script("lib/helpers.sqrl");
    script("main.sqrl");
    base("package.json", "main.sqrl");
    // A package of functions or tables without a main script, and a package of npm
    Files.writeString(dir.resolve("lib/package.json"), "{\"version\": \"1\", \"script\": {}}");
    Files.writeString(dir.resolve("lib/tsconfig.package.json"), "{\"name\": \"lib\"}");

    assertThat(find(helper)).containsExactly("package.json");
  }

  @Test
  void givenConfiguredPackageFiles_whenFind_thenFindsTheDirectoryWithTheFirst() throws IOException {
    var script = script("project/main.sqrl");
    base("project/package.json", "main.sqrl");
    overlay("project/test-package.json");
    base("project/other_package.json", "main.sqrl");

    assertThat(ProjectPackages.find(script, List.of("package.json", "test-package.json")))
        .containsExactly(
            dir.resolve("project/package.json"), dir.resolve("project/test-package.json"));
  }

  @Test
  void givenPackageFileChangedSinceTheLastFind_whenFind_thenUsesTheChange() throws IOException {
    var main = script("main.sqrl");
    var other = script("other.sqrl");
    base("package.json", "main.sqrl");
    assertThat(ProjectPackages.candidates(other)).isEmpty();

    base("package.json", "other.sqrl");
    // As a later write would, also on file systems whose times are in seconds
    Files.setLastModifiedTime(
        dir.resolve("package.json"), FileTime.fromMillis(System.currentTimeMillis() + 10_000));

    assertThat(ProjectPackages.candidates(other)).containsExactly(dir.resolve("package.json"));
    assertThat(ProjectPackages.candidates(main)).isEmpty();
  }

  @Test
  void givenPackageFileAddedSinceTheLastFind_whenFind_thenFindsIt() throws IOException {
    var script = script("main.sqrl");
    assertThat(find(script)).isEmpty();

    base("package.json", "main.sqrl");
    Files.setLastModifiedTime(dir, FileTime.fromMillis(System.currentTimeMillis() + 10_000));

    assertThat(find(script)).containsExactly("package.json");
  }

  @Test
  void givenNoPackage_whenFind_thenFindsNone() throws IOException {
    assertThat(find(script("loose.sqrl"))).isEmpty();
  }

  private List<String> find(Path script) {
    return ProjectPackages.find(script, List.of()).stream()
        .map(file -> dir.relativize(file).toString())
        .toList();
  }

  private Path script(String path) throws IOException {
    var script = dir.resolve(path);
    Files.createDirectories(script.getParent());
    return Files.writeString(script, "X := SELECT 1 AS one;\n");
  }

  private void base(String path, String main) throws IOException {
    Files.writeString(
        dir.resolve(path), "{\"version\": \"1\", \"script\": {\"main\": \"%s\"}}".formatted(main));
  }

  private void overlay(String path) throws IOException {
    Files.writeString(dir.resolve(path), "{\"test-runner\": {\"delay-sec\": 1}}");
  }
}
