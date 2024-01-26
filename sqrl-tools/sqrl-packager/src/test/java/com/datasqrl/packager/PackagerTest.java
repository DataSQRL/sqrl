/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import static com.datasqrl.packager.Packager.BUILD_DIR_NAME;
import static com.datasqrl.packager.Packager.PACKAGE_JSON;
import static com.datasqrl.packager.Packager.setScriptFiles;
import static com.datasqrl.packager.config.ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.config.ConfigurationTest;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class PackagerTest {

  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @Test
  public void testRetailPackaging() {
    TestScript script = Retail.INSTANCE.getScript(Retail.RetailScriptNames.FULL);
    Path graphQLSchema = script.getRootPackageDirectory().resolve("c360-full-graphqlv1")
        .resolve(GRAPHQL_NORMALIZED_FILE_NAME);
    Path packageFileWithoutScript = script.getRootPackageDirectory()
        .resolve("package-exampleWOscript.json");
    Path packageFileWithScript = script.getRootPackageDirectory()
        .resolve("package-exampleWscript.json");

    testCombination(script.getScriptPath(), null, packageFileWithoutScript);
    testCombination(script.getScriptPath(), null, packageFileWithScript);
    testCombination(null, null, packageFileWithScript);
    testCombination(script.getScriptPath(), graphQLSchema, packageFileWithoutScript);
    testCombination(script.getScriptPath(), graphQLSchema, packageFileWithScript);
    testCombination(null, graphQLSchema, packageFileWithScript);

    snapshot.createOrValidate();
  }

  private static final Path baseDependencyPath = ConfigurationTest.RESOURCE_DIR.resolve("dependency");
  private static final List<Path> depScripts = new ArrayList<>();
  static {
    IntStream.rangeClosed(1,2)
        .forEach(i -> depScripts.add(baseDependencyPath.resolve("main" + i + ".sqrl")));
  }
  private static final Path pkgWDeps = baseDependencyPath.resolve("packageWDependencies.json");
  private static final Path pkgWODeps = baseDependencyPath.resolve("packageWODependencies.json");

  @Test
  public void dependencyResolution() {
    testCombinationMockRepo(depScripts.get(0),pkgWDeps);
    testCombinationMockRepo(depScripts.get(0),pkgWODeps);
    testCombinationMockRepo(depScripts.get(1),pkgWDeps);
    testCombinationMockRepo(depScripts.get(1),pkgWODeps);
    snapshot.createOrValidate();
  }

  @Test
  public void testRemoteRepository() {
    testCombination(depScripts.get(0), null, pkgWODeps);
    testCombination(depScripts.get(0), null, pkgWDeps);
    snapshot.createOrValidate();
  }

  private void testCombinationMockRepo(Path main, Path packageFile) {
    testCombination(main,null,packageFile,new MockRepository());
  }

  private void testCombination(Path main, Path graphQl, Path packageFile) {
    testCombination(main,graphQl,packageFile,new MockRepository());
  }

  @SneakyThrows
  private void testCombination(Path main, Path graphQl, Path packageFile, Repository repository) {
    ErrorCollector errors = ErrorCollector.root();
    SqrlConfig sqrlConfig = SqrlConfigCommons.fromFiles(errors, packageFile);
    setScriptFiles(packageFile.getParent(), main, graphQl, sqrlConfig, errors);
    Packager pkg = new Packager(repository, packageFile.getParent(), sqrlConfig, errors);
    Path buildDir = packageFile.getParent().resolve(BUILD_DIR_NAME);
    pkg.cleanBuildDir(buildDir);
    populateBuildDirAndTakeSnapshot(pkg, main, graphQl, packageFile);
    pkg.cleanBuildDir(buildDir);
  }

  @SneakyThrows
  private void populateBuildDirAndTakeSnapshot(Packager pkg, Path main, Path graphQl, Path packageFile) {
    Path buildDir = pkg.getRootDir().resolve(Packager.BUILD_DIR_NAME);
    pkg.preprocess(true);
    String[] caseNames = Stream.of(main, graphQl, packageFile)
        .filter(Predicate.not(Objects::isNull)).map(String::valueOf)
        .toArray(size -> new String[size + 1]);
    caseNames[caseNames.length - 1] = "dir";
    snapshot.addContent(FileTestUtil.getAllFilesAsString(buildDir), caseNames);
    caseNames[caseNames.length - 1] = "package";
    snapshot.addContent(Files.readString(buildDir.resolve(PACKAGE_JSON)), caseNames);
  }

  private static class MockRepository implements Repository {

    @Override
    public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
      assertEquals(NUTSHOP, dependency);
      Files.createDirectories(targetPath);
      Files.writeString(targetPath.resolve("package.json"),"test");
      return true;
    }

    @Override
    public Optional<Dependency> resolveDependency(String packageName) {
      if (NUTSHOP.getName().equals(packageName)) {
        return Optional.of(NUTSHOP);
      }
      return Optional.empty();
    }

    private static final Dependency NUTSHOP = new Dependency("datasqrl.examples.Nutshop","0.1.0","dev");
  }
}
